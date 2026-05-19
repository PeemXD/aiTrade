package riskengine

import (
	"context"
	"time"

	"github.com/local/polymarket-process-service/pkg/config"
	"github.com/local/polymarket-process-service/pkg/eventbus"
	"github.com/local/polymarket-process-service/pkg/idgen"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	riskmodel "github.com/local/polymarket-process-service/pkg/model/risk"
	"github.com/local/polymarket-process-service/pkg/repository"
)

type Engine struct {
	cfg   config.Config
	store repository.Store
	bus   eventbus.Bus
}

func NewEngine(cfg config.Config, store repository.Store) *Engine {
	return &Engine{cfg: cfg, store: store}
}

func (e *Engine) SetEventBus(bus eventbus.Bus) {
	e.bus = bus
}

func (e *Engine) Evaluate(ctx context.Context, req riskmodel.EvaluateRequest) (riskmodel.RiskDecision, error) {
	prob, err := e.store.GetProbabilityDecision(ctx, req.ProbabilityDecisionID)
	if err != nil {
		return riskmodel.RiskDecision{}, err
	}
	market, err := e.store.GetMarket(ctx, prob.MarketID)
	if err != nil {
		return riskmodel.RiskDecision{}, err
	}
	market = e.withLiveState(ctx, market)
	portfolio, err := e.store.Portfolio(ctx, e.cfg.PaperStartingBalanceUSD)
	if err != nil {
		return riskmodel.RiskDecision{}, err
	}
	checks := map[string]any{}
	reject := func(reason string) (riskmodel.RiskDecision, error) {
		d := riskmodel.RiskDecision{ID: idgen.New(), MarketID: market.ID, ProbabilityDecisionID: prob.ID, Approved: false, Checks: checks, RejectReason: reason, CreatedAt: time.Now().UTC()}
		_ = e.store.SaveRiskDecision(ctx, d)
		_ = e.store.SaveAudit(ctx, repository.AuditLog{ID: idgen.New(), Event: "risk_rejected", EntityID: market.ID, Payload: map[string]any{"reason": reason}, CreatedAt: d.CreatedAt})
		if e.bus != nil {
			e.bus.Publish(ctx, eventbus.Event{Topic: "risk.rejected", Data: d})
		}
		return d, nil
	}
	if portfolio.DailyPnLPct <= -e.cfg.MaxDailyLossPct {
		checks["daily_loss"] = false
		return reject("daily loss exceeded")
	}
	checks["daily_loss"] = true
	open, _ := e.store.ListPositions(ctx, "open")
	if len(open) >= e.cfg.MaxTotalOpenPositions {
		checks["open_positions"] = false
		return reject("max total open positions reached")
	}
	checks["open_positions"] = true
	if market.Spread > e.cfg.MarketMaxSpread {
		checks["spread"] = false
		return reject("spread too wide")
	}
	checks["spread"] = true
	if market.Liquidity < e.cfg.MarketMinLiquidityUSD {
		checks["liquidity"] = false
		return reject("insufficient liquidity")
	}
	checks["liquidity"] = true
	if !market.EndTime.IsZero() && time.Until(market.EndTime).Hours() < e.cfg.MarketMinHoursToExpiry {
		checks["expiry"] = false
		return reject("market too close to expiry")
	}
	checks["expiry"] = true
	existingMarketExposure := 0.0
	categoryExposure := 0.0
	for _, p := range open {
		if p.MarketID == market.ID {
			existingMarketExposure += p.ExposureUSD
		}
		pm, err := e.store.GetMarket(ctx, p.MarketID)
		if err == nil && pm.Category == market.Category {
			categoryExposure += p.ExposureUSD
		}
	}
	maxMarketExposure := e.cfg.MaxPositionPerMarketPct * portfolio.PortfolioValueUSD
	maxCategoryExposure := e.cfg.MaxCategoryExposurePct * portfolio.PortfolioValueUSD
	if existingMarketExposure >= maxMarketExposure {
		checks["market_exposure"] = false
		return reject("market exposure too high")
	}
	checks["market_exposure"] = true
	if categoryExposure >= maxCategoryExposure {
		checks["category_exposure"] = false
		return reject("category exposure too high")
	}
	checks["category_exposure"] = true
	kellyPct := kellyFraction(prob.OurProbability, prob.ExecutablePrice)
	checks["kelly_fraction"] = kellyPct
	if kellyPct <= 0 {
		return reject("kelly sizing non-positive")
	}
	sizePct := min(kellyPct*e.cfg.KellyFraction, e.cfg.MaxPositionPerMarketPct)
	sizeUSD := portfolio.PortfolioValueUSD * sizePct
	if req.RequestedSizeUSD > 0 && req.RequestedSizeUSD < sizeUSD {
		sizeUSD = req.RequestedSizeUSD
	}
	if existingMarketExposure+sizeUSD > maxMarketExposure {
		checks["market_exposure"] = false
		return reject("market exposure too high")
	}
	if categoryExposure+sizeUSD > maxCategoryExposure {
		checks["category_exposure"] = false
		return reject("category exposure too high")
	}
	impact := priceImpact(sizeUSD, prob.ExecutablePrice, market.OrderBook.Asks)
	checks["orderbook_impact_pct"] = impact
	if impact > e.cfg.MaxOrderbookImpactPct {
		return reject("orderbook impact too high")
	}
	d := riskmodel.RiskDecision{
		ID: idgen.New(), MarketID: market.ID, ProbabilityDecisionID: prob.ID, Approved: true,
		PositionSizeUSD: sizeUSD, MaxLossUSD: sizeUSD, Checks: checks, Reason: "edge, liquidity, exposure, and Kelly sizing passed", CreatedAt: time.Now().UTC(),
	}
	if err := e.store.SaveRiskDecision(ctx, d); err != nil {
		return d, err
	}
	_ = e.store.SaveAudit(ctx, repository.AuditLog{ID: idgen.New(), Event: "risk_approved", EntityID: market.ID, Payload: map[string]any{"position_size_usd": d.PositionSizeUSD}, CreatedAt: d.CreatedAt})
	if e.bus != nil {
		e.bus.Publish(ctx, eventbus.Event{Topic: "risk.approved", Data: d})
	}
	return d, nil
}

func (e *Engine) withLiveState(ctx context.Context, market marketmodel.Market) marketmodel.Market {
	state, err := e.store.GetLiveMarketState(ctx, market.ID)
	if err != nil {
		return market
	}
	return marketmodel.ApplyLiveState(market, state)
}

func kellyFraction(p, cost float64) float64 {
	if cost <= 0 || cost >= 1 {
		return -1
	}
	b := (1 - cost) / cost
	q := 1 - p
	return (b*p - q) / b
}

func priceImpact(sizeUSD, expectedPrice float64, levels []marketmodel.OrderBookLevel) float64 {
	if sizeUSD <= 0 || expectedPrice <= 0 || len(levels) == 0 {
		return 0
	}
	remaining := sizeUSD
	filledUSD := 0.0
	quantity := 0.0
	for _, level := range levels {
		if level.Price <= 0 || level.Size <= 0 {
			continue
		}
		maxSpend := level.Price * level.Size
		spend := min(remaining, maxSpend)
		shares := spend / level.Price
		quantity += shares
		filledUSD += spend
		remaining -= spend
		if remaining <= 0 {
			break
		}
	}
	if quantity == 0 {
		return 1
	}
	avg := filledUSD / quantity
	if avg <= expectedPrice {
		return 0
	}
	return (avg - expectedPrice) / expectedPrice
}
