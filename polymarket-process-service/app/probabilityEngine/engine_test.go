package probabilityengine

import (
	"context"
	"testing"
	"time"

	"github.com/local/polymarket-process-service/pkg/config"
	aimodel "github.com/local/polymarket-process-service/pkg/model/ai"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	probmodel "github.com/local/polymarket-process-service/pkg/model/probability"
	"github.com/local/polymarket-process-service/pkg/repository"
	"github.com/stretchr/testify/require"
)

func TestProbabilityWeightedAIDeltaAndPricedInMultiplier(t *testing.T) {
	ctx := context.Background()
	store := repository.NewMemoryStore()
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{{ID: "m1", Question: "Will ETH ETF be approved?", BestBid: 0.61, BestAsk: 0.63, YesPrice: 0.62}}))
	require.NoError(t, store.SaveAISignal(ctx, aimodel.AISignal{ID: "s1", MarketID: "m1", NewsID: "n1", ProbabilityDelta: 0.10, Confidence: 0.80, SourceReliability: 0.50, PricedInRisk: "medium", CreatedAt: time.Now()}))

	engine := NewEngine(testConfig(), store)
	decision, err := engine.Calculate(ctx, probmodel.CalculateRequest{MarketID: "m1", NewsID: "n1"})

	require.NoError(t, err)
	require.InDelta(t, 0.644, decision.OurProbability, 0.0001)
	require.InDelta(t, 0.014, decision.Edge, 0.0001)
	require.Equal(t, probmodel.DecisionNoTrade, decision.Decision)
}

func TestProbabilityClamp(t *testing.T) {
	ctx := context.Background()
	store := repository.NewMemoryStore()
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{{ID: "m1", Question: "Will BTC rally?", BestBid: 0.94, BestAsk: 0.96, YesPrice: 0.95}}))
	require.NoError(t, store.SaveAISignal(ctx, aimodel.AISignal{ID: "s1", MarketID: "m1", NewsID: "n1", ProbabilityDelta: 0.20, Confidence: 1, SourceReliability: 1, PricedInRisk: "low", CreatedAt: time.Now()}))

	decision, err := NewEngine(testConfig(), store).Calculate(ctx, probmodel.CalculateRequest{MarketID: "m1", NewsID: "n1"})

	require.NoError(t, err)
	require.Equal(t, 0.99, decision.OurProbability)
}

func TestProbabilityEdgeCalculationCandidate(t *testing.T) {
	ctx := context.Background()
	store := repository.NewMemoryStore()
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{{ID: "m1", Question: "Will ETH ETF be approved?", BestBid: 0.50, BestAsk: 0.52, YesPrice: 0.51}}))
	require.NoError(t, store.SaveAISignal(ctx, aimodel.AISignal{ID: "s1", MarketID: "m1", NewsID: "n1", ProbabilityDelta: 0.20, Confidence: 0.90, SourceReliability: 1, PricedInRisk: "low", CreatedAt: time.Now()}))

	decision, err := NewEngine(testConfig(), store).Calculate(ctx, probmodel.CalculateRequest{MarketID: "m1", NewsID: "n1"})

	require.NoError(t, err)
	require.Equal(t, probmodel.DecisionCandidate, decision.Decision)
	require.Greater(t, decision.Edge, 0.05)
}

func TestProbabilityUsesLiveMarketState(t *testing.T) {
	ctx := context.Background()
	store := repository.NewMemoryStore()
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{{ID: "m1", Question: "Will BTC rally?", BestBid: 0.40, BestAsk: 0.42, YesPrice: 0.41}}))
	require.NoError(t, store.SaveLiveMarketState(ctx, marketmodel.LiveMarketState{MarketID: "m1", BestBid: 0.60, BestAsk: 0.62, MidPrice: 0.61, Spread: 0.02, UpdatedAt: time.Now()}))
	require.NoError(t, store.SaveAISignal(ctx, aimodel.AISignal{ID: "s1", MarketID: "m1", NewsID: "n1", ProbabilityDelta: 0, Confidence: 0.8, SourceReliability: 1, PricedInRisk: "low", CreatedAt: time.Now()}))

	decision, err := NewEngine(testConfig(), store).Calculate(ctx, probmodel.CalculateRequest{MarketID: "m1", NewsID: "n1"})

	require.NoError(t, err)
	require.InDelta(t, 0.61, decision.MarketProbability, 0.0001)
	require.InDelta(t, 0.62, decision.ExecutablePrice, 0.0001)
}

func testConfig() config.Config {
	return config.Config{EdgeThreshold: 0.05, MinSignalConfidence: 0.65}
}
