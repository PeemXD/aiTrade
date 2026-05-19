package probabilityengine

import (
	"context"
	"math"
	"time"

	"github.com/local/polymarket-process-service/pkg/config"
	"github.com/local/polymarket-process-service/pkg/eventbus"
	"github.com/local/polymarket-process-service/pkg/idgen"
	aimodel "github.com/local/polymarket-process-service/pkg/model/ai"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	probmodel "github.com/local/polymarket-process-service/pkg/model/probability"
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

func (e *Engine) Calculate(ctx context.Context, req probmodel.CalculateRequest) (probmodel.ProbabilityDecision, error) {
	market, err := e.store.GetMarket(ctx, req.MarketID)
	if err != nil {
		return probmodel.ProbabilityDecision{}, err
	}
	priorMarketProbability := marketProbability(market.BestBid, market.BestAsk, market.YesPrice)
	market, hasLiveState := e.withLiveState(ctx, market)
	signal, _ := e.store.LatestAISignal(ctx, req.MarketID, req.NewsID)
	side := req.Side
	if side == "" {
		side = "buy"
	}
	outcome := req.Outcome
	if outcome == "" {
		outcome = "yes"
	}
	marketProb := marketProbability(market.BestBid, market.BestAsk, market.YesPrice)
	execPrice := executablePrice(outcome, side, market.BestBid, market.BestAsk, market.YesPrice, market.NoPrice)
	effectiveAIDelta := signal.ProbabilityDelta * signal.Confidence * signal.SourceReliability * pricedInMultiplier(signal.PricedInRisk)
	priceMomentum := recentPriceMomentum(priorMarketProbability, marketProb, hasLiveState)
	momentumDelta := priceMomentum * 0.30
	ourProb := clamp(marketProb+effectiveAIDelta+momentumDelta, 0.01, 0.99)
	edge := ourProb - execPrice
	if outcome == "no" {
		edge = (1 - ourProb) - execPrice
	}
	if side == "sell" {
		edge = execPrice - ourProb
	}
	confidence := signal.Confidence
	decision := probmodel.DecisionNoTrade
	reason := "edge/confidence below threshold"
	if math.Abs(edge) >= e.cfg.EdgeThreshold && confidence >= e.cfg.MinSignalConfidence {
		decision = probmodel.DecisionCandidate
		reason = "positive edge after AI/news adjustment"
	}
	if signal.Disabled {
		reason = "AI disabled / missing key"
	}
	d := probmodel.ProbabilityDecision{
		ID: idgen.New(), MarketID: market.ID, NewsID: req.NewsID, MarketProbability: marketProb,
		ExecutablePrice: execPrice, OurProbability: ourProb, Edge: edge, Confidence: confidence,
		Components: map[string]any{
			"ai_delta": signal.ProbabilityDelta, "ai_confidence": signal.Confidence, "source_reliability": signal.SourceReliability,
			"priced_in_risk": signal.PricedInRisk, "priced_in_multiplier": pricedInMultiplier(signal.PricedInRisk),
			"effective_ai_delta": effectiveAIDelta, "recent_price_momentum": priceMomentum, "momentum_delta": momentumDelta,
		},
		Decision: decision, Reason: reason, Side: side, Outcome: outcome, CreatedAt: time.Now().UTC(),
	}
	if err := e.store.SaveProbabilityDecision(ctx, d); err != nil {
		return d, err
	}
	_ = e.store.SaveAudit(ctx, repository.AuditLog{ID: idgen.New(), Event: "probability_calculated", EntityID: market.ID, Payload: map[string]any{"market_probability": d.MarketProbability, "our_probability": d.OurProbability, "edge": d.Edge, "decision": d.Decision}, CreatedAt: d.CreatedAt})
	_ = e.store.SaveAudit(ctx, repository.AuditLog{ID: idgen.New(), Event: "probability_" + string(d.Decision), EntityID: market.ID, Payload: map[string]any{"probability_decision_id": d.ID, "edge": d.Edge, "reason": d.Reason}, CreatedAt: d.CreatedAt})
	if e.bus != nil {
		if d.Decision == probmodel.DecisionCandidate {
			e.bus.Publish(ctx, eventbus.Event{Topic: "probability.candidate", Data: d})
		} else {
			e.bus.Publish(ctx, eventbus.Event{Topic: "probability.no_trade", Data: d})
		}
	}
	return d, nil
}

func (e *Engine) withLiveState(ctx context.Context, market marketmodel.Market) (marketmodel.Market, bool) {
	state, err := e.store.GetLiveMarketState(ctx, market.ID)
	if err != nil {
		return market, false
	}
	return marketmodel.ApplyLiveState(market, state), true
}

func marketProbability(bid, ask, fallback float64) float64 {
	if bid > 0 && ask > 0 {
		return (bid + ask) / 2
	}
	if fallback > 0 {
		return fallback
	}
	return 0.5
}

func executablePrice(outcome, side string, bid, ask, yesPrice, noPrice float64) float64 {
	if outcome == "no" {
		if side == "buy" && noPrice > 0 {
			return noPrice
		}
		if side == "sell" && noPrice > 0 {
			return noPrice
		}
		if side == "buy" && bid > 0 {
			return 1 - bid
		}
		if ask > 0 {
			return 1 - ask
		}
		return 0.5
	}
	if side == "sell" && bid > 0 {
		return bid
	}
	if ask > 0 {
		return ask
	}
	if yesPrice > 0 {
		return yesPrice
	}
	return 0.5
}

func pricedInMultiplier(risk string) float64 {
	switch risk {
	case "low":
		return 1.0
	case "medium":
		return 0.6
	case "high":
		return 0.3
	default:
		return 0.6
	}
}

func recentPriceMomentum(priorProbability, currentProbability float64, hasLiveState bool) float64 {
	if !hasLiveState || priorProbability <= 0 || currentProbability <= 0 {
		return 0
	}
	return currentProbability - priorProbability
}

func clamp(v, minV, maxV float64) float64 {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

var _ = aimodel.DirectionNeutral
