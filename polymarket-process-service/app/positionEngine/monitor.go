package positionengine

import (
	"context"
	"math"
	"time"

	"github.com/local/polymarket-process-service/pkg/config"
	"github.com/local/polymarket-process-service/pkg/eventbus"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	positionmodel "github.com/local/polymarket-process-service/pkg/model/position"
	"github.com/local/polymarket-process-service/pkg/repository"
)

type Monitor struct {
	cfg   config.Config
	store repository.Store
	bus   eventbus.Bus
}

func NewMonitor(cfg config.Config, store repository.Store) *Monitor {
	return &Monitor{cfg: cfg, store: store}
}

func (m *Monitor) SetEventBus(bus eventbus.Bus) {
	m.bus = bus
}

func (m *Monitor) Evaluate(position positionmodel.Position, currentEdge float64, dailyLossExceeded bool, endTime time.Time) positionmodel.ExitDecision {
	pnlPct := 0.0
	if position.ExposureUSD > 0 {
		pnlPct = position.UnrealizedPnLUSD / position.ExposureUSD
	}
	decision := func(shouldExit bool, reason string) positionmodel.ExitDecision {
		return positionmodel.ExitDecision{
			ShouldExit: shouldExit,
			PositionID: position.ID,
			MarketID:   position.MarketID,
			Reason:     reason,
			PnLPct:     pnlPct,
			Edge:       currentEdge,
		}
	}
	switch {
	case math.Abs(currentEdge) < m.cfg.EdgeThreshold*0.5:
		return decision(true, "edge_gone")
	case pnlPct >= m.cfg.TakeProfitPct:
		return decision(true, "take_profit")
	case pnlPct <= -m.cfg.StopLossPct:
		return decision(true, "stop_loss")
	case dailyLossExceeded:
		return decision(true, "risk_exceeded")
	case !endTime.IsZero() && time.Until(endTime).Hours() < m.cfg.ExitBeforeExpiryHours:
		return decision(true, "expiry_close")
	default:
		return decision(false, "hold")
	}
}

func (m *Monitor) UpdateOpenPositions(ctx context.Context) ([]positionmodel.ExitDecision, error) {
	positions, err := m.store.ListPositions(ctx, "open")
	if err != nil {
		return nil, err
	}
	portfolio, err := m.store.Portfolio(ctx, m.cfg.PaperStartingBalanceUSD)
	if err != nil {
		return nil, err
	}
	out := []positionmodel.ExitDecision{}
	for _, p := range positions {
		market, err := m.store.GetMarket(ctx, p.MarketID)
		if err != nil {
			continue
		}
		market = m.withLiveState(ctx, market)
		price := market.BestBid
		if price == 0 {
			price = market.YesPrice
		}
		p.CurrentPrice = price
		p.UnrealizedPnLUSD = (price - p.AvgEntryPrice) * p.Quantity
		p.UpdatedAt = time.Now().UTC()
		_ = m.store.SavePosition(ctx, p)
		m.publish(ctx, "position.updated", p)
		decision := m.Evaluate(p, m.latestEdge(ctx, p.MarketID), portfolio.DailyPnLPct <= -m.cfg.MaxDailyLossPct, market.EndTime)
		if decision.ShouldExit {
			m.publish(ctx, "position.exit_candidate", decision)
		}
		out = append(out, decision)
	}
	return out, nil
}

func (m *Monitor) publish(ctx context.Context, topic string, data any) {
	if m.bus == nil {
		return
	}
	m.bus.Publish(ctx, eventbus.Event{Topic: topic, Data: data})
}

func (m *Monitor) withLiveState(ctx context.Context, market marketmodel.Market) marketmodel.Market {
	state, err := m.store.GetLiveMarketState(ctx, market.ID)
	if err != nil {
		return market
	}
	return marketmodel.ApplyLiveState(market, state)
}

func (m *Monitor) latestEdge(ctx context.Context, marketID string) float64 {
	decisions, err := m.store.ListProbabilityDecisions(ctx, 100)
	if err != nil {
		return 1
	}
	found := false
	latestEdge := 1.0
	latestAt := time.Time{}
	for _, decision := range decisions {
		if decision.MarketID != marketID {
			continue
		}
		if !found || decision.CreatedAt.After(latestAt) {
			found = true
			latestAt = decision.CreatedAt
			latestEdge = decision.Edge
		}
	}
	return latestEdge
}
