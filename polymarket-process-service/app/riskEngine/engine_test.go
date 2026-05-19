package riskengine

import (
	"context"
	"testing"
	"time"

	"github.com/local/polymarket-process-service/pkg/config"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	positionmodel "github.com/local/polymarket-process-service/pkg/model/position"
	probmodel "github.com/local/polymarket-process-service/pkg/model/probability"
	riskmodel "github.com/local/polymarket-process-service/pkg/model/risk"
	"github.com/local/polymarket-process-service/pkg/repository"
	"github.com/stretchr/testify/require"
)

func TestRiskRejectDailyLossExceeded(t *testing.T) {
	ctx, store := riskFixture(t)
	require.NoError(t, store.SavePosition(ctx, positionmodel.Position{ID: "p1", MarketID: "m1", Outcome: "yes", Quantity: 100, AvgEntryPrice: 0.50, CurrentPrice: 0.10, ExposureUSD: 1000, UnrealizedPnLUSD: -400, Status: "open", OpenedAt: time.Now(), UpdatedAt: time.Now()}))

	decision, err := NewEngine(riskConfig(), store).Evaluate(ctx, riskmodel.EvaluateRequest{ProbabilityDecisionID: "pd1"})

	require.NoError(t, err)
	require.False(t, decision.Approved)
	require.Equal(t, "daily loss exceeded", decision.RejectReason)
}

func TestRiskRejectSpreadTooWide(t *testing.T) {
	ctx, store := riskFixture(t)
	market, _ := store.GetMarket(ctx, "m1")
	market.Spread = 0.10
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{market}))

	decision, err := NewEngine(riskConfig(), store).Evaluate(ctx, riskmodel.EvaluateRequest{ProbabilityDecisionID: "pd1"})

	require.NoError(t, err)
	require.False(t, decision.Approved)
	require.Equal(t, "spread too wide", decision.RejectReason)
}

func TestRiskRejectSpreadTooWideFromLiveState(t *testing.T) {
	ctx, store := riskFixture(t)
	require.NoError(t, store.SaveLiveMarketState(ctx, marketmodel.LiveMarketState{
		MarketID: "m1", BestBid: 0.40, BestAsk: 0.50, MidPrice: 0.45, Spread: 0.10,
		UpdatedAt: time.Now(),
	}))

	decision, err := NewEngine(riskConfig(), store).Evaluate(ctx, riskmodel.EvaluateRequest{ProbabilityDecisionID: "pd1"})

	require.NoError(t, err)
	require.False(t, decision.Approved)
	require.Equal(t, "spread too wide", decision.RejectReason)
}

func TestRiskRejectInsufficientLiquidity(t *testing.T) {
	ctx, store := riskFixture(t)
	market, _ := store.GetMarket(ctx, "m1")
	market.Liquidity = 100
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{market}))

	decision, err := NewEngine(riskConfig(), store).Evaluate(ctx, riskmodel.EvaluateRequest{ProbabilityDecisionID: "pd1"})

	require.NoError(t, err)
	require.False(t, decision.Approved)
	require.Equal(t, "insufficient liquidity", decision.RejectReason)
}

func TestRiskRejectExposureTooHigh(t *testing.T) {
	ctx, store := riskFixture(t)
	require.NoError(t, store.SavePosition(ctx, positionmodel.Position{ID: "p1", MarketID: "m1", Outcome: "yes", Quantity: 100, AvgEntryPrice: 0.50, CurrentPrice: 0.50, ExposureUSD: 500, Status: "open", OpenedAt: time.Now(), UpdatedAt: time.Now()}))

	decision, err := NewEngine(riskConfig(), store).Evaluate(ctx, riskmodel.EvaluateRequest{ProbabilityDecisionID: "pd1"})

	require.NoError(t, err)
	require.False(t, decision.Approved)
	require.Equal(t, "market exposure too high", decision.RejectReason)
}

func TestRiskApproveValidTradeAndKellySizing(t *testing.T) {
	ctx, store := riskFixture(t)

	decision, err := NewEngine(riskConfig(), store).Evaluate(ctx, riskmodel.EvaluateRequest{ProbabilityDecisionID: "pd1"})

	require.NoError(t, err)
	require.True(t, decision.Approved)
	require.InDelta(t, 500, decision.PositionSizeUSD, 0.01)
	require.Equal(t, "edge, liquidity, exposure, and Kelly sizing passed", decision.Reason)
}

func TestRiskKellySizingBelowMarketCap(t *testing.T) {
	ctx, store := riskFixture(t)
	require.NoError(t, store.SaveProbabilityDecision(ctx, probmodel.ProbabilityDecision{ID: "pd2", MarketID: "m1", ExecutablePrice: 0.50, OurProbability: 0.56, Edge: 0.06, Confidence: 0.8, Decision: probmodel.DecisionCandidate, Outcome: "yes", Side: "buy", CreatedAt: time.Now()}))

	decision, err := NewEngine(riskConfig(), store).Evaluate(ctx, riskmodel.EvaluateRequest{ProbabilityDecisionID: "pd2"})

	require.NoError(t, err)
	require.True(t, decision.Approved)
	require.InDelta(t, 300, decision.PositionSizeUSD, 0.01)
}

func riskFixture(t *testing.T) (context.Context, *repository.MemoryStore) {
	t.Helper()
	ctx := context.Background()
	store := repository.NewMemoryStore()
	market := marketmodel.Market{
		ID: "m1", Question: "Will ETH ETF be approved?", Category: "crypto", Active: true,
		EndTime: time.Now().Add(48 * time.Hour), Volume: 100000, Liquidity: 50000, BestBid: 0.49, BestAsk: 0.50, Spread: 0.01,
		OrderBook: marketmodel.OrderBook{Asks: []marketmodel.OrderBookLevel{{Price: 0.50, Size: 5000}}},
	}
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{market}))
	require.NoError(t, store.SaveProbabilityDecision(ctx, probmodel.ProbabilityDecision{ID: "pd1", MarketID: "m1", ExecutablePrice: 0.50, OurProbability: 0.65, Edge: 0.15, Confidence: 0.9, Decision: probmodel.DecisionCandidate, Outcome: "yes", Side: "buy", CreatedAt: time.Now()}))
	return ctx, store
}

func riskConfig() config.Config {
	return config.Config{
		PaperStartingBalanceUSD: 10000, MaxDailyLossPct: 0.03, MaxTotalOpenPositions: 10, MarketMaxSpread: 0.05,
		MarketMinLiquidityUSD: 10000, MarketMinHoursToExpiry: 12, MaxPositionPerMarketPct: 0.05,
		MaxCategoryExposurePct: 0.20, KellyFraction: 0.25, MaxOrderbookImpactPct: 0.02,
	}
}
