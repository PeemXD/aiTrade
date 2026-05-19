package executionengine

import (
	"context"
	"testing"
	"time"

	execmodel "github.com/local/polymarket-process-service/pkg/model/execution"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	"github.com/local/polymarket-process-service/pkg/repository"
	"github.com/stretchr/testify/require"
)

func TestPaperExecutionSimulatesFillFromOrderbook(t *testing.T) {
	ctx, store := paperFixture(t)
	provider := NewPaperExecutionProvider(store)

	result, err := provider.PlaceOrder(ctx, execmodel.OrderRequest{MarketID: "m1", Outcome: "yes", Side: "buy", LimitPrice: 0.55, SizeUSD: 75, Reason: "test"})

	require.NoError(t, err)
	require.Equal(t, "open", result.Status)
	require.InDelta(t, 75, result.FilledSizeUSD, 0.001)
	require.InDelta(t, 145.45, result.Quantity, 0.01)
	require.InDelta(t, 0.5156, result.AveragePrice, 0.001)
}

func TestPaperExecutionPartialFillHandling(t *testing.T) {
	ctx := context.Background()
	store := repository.NewMemoryStore()
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{{ID: "m1", BestAsk: 0.50, OrderBook: marketmodel.OrderBook{Asks: []marketmodel.OrderBookLevel{{Price: 0.50, Size: 50}}}}}))
	provider := NewPaperExecutionProvider(store)

	result, err := provider.PlaceOrder(ctx, execmodel.OrderRequest{MarketID: "m1", Outcome: "yes", Side: "buy", LimitPrice: 0.50, SizeUSD: 100, Reason: "test"})

	require.NoError(t, err)
	require.Equal(t, "open", result.Status)
	require.InDelta(t, 25, result.FilledSizeUSD, 0.001)
	require.InDelta(t, 50, result.Quantity, 0.001)
}

func TestPaperExecutionSlippageCalculation(t *testing.T) {
	fill := simulateFill(75, 0, "buy", []marketmodel.OrderBookLevel{{Price: 0.50, Size: 100}, {Price: 0.55, Size: 100}})

	require.Greater(t, fill.SlippageUSD, 0.0)
}

func TestPaperExecutionPersistsPosition(t *testing.T) {
	ctx, store := paperFixture(t)
	provider := NewPaperExecutionProvider(store)

	_, err := provider.PlaceOrder(ctx, execmodel.OrderRequest{MarketID: "m1", Outcome: "yes", Side: "buy", LimitPrice: 0.55, SizeUSD: 50, Reason: "test"})

	require.NoError(t, err)
	positions, err := store.ListPositions(ctx, "open")
	require.NoError(t, err)
	require.Len(t, positions, 1)
	require.Equal(t, "m1", positions[0].MarketID)
	require.WithinDuration(t, time.Now(), positions[0].OpenedAt, time.Second)
}

func TestPaperExecutionCloseUpdatesOpenTrade(t *testing.T) {
	ctx, store := paperFixture(t)
	provider := NewPaperExecutionProvider(store)
	openResult, err := provider.PlaceOrder(ctx, execmodel.OrderRequest{MarketID: "m1", Outcome: "yes", Side: "buy", LimitPrice: 0.55, SizeUSD: 50, Reason: "test"})
	require.NoError(t, err)
	positions, err := store.ListPositions(ctx, "open")
	require.NoError(t, err)
	require.Len(t, positions, 1)

	closeResult, err := provider.ClosePosition(ctx, positions[0], "edge_gone")

	require.NoError(t, err)
	require.Equal(t, openResult.TradeID, closeResult.TradeID)
	trade, err := store.GetTrade(ctx, openResult.TradeID)
	require.NoError(t, err)
	require.Equal(t, "closed", trade.Status)
	require.NotZero(t, trade.ExitPrice)
	positions, err = store.ListPositions(ctx, "open")
	require.NoError(t, err)
	require.Empty(t, positions)
}

func paperFixture(t *testing.T) (context.Context, *repository.MemoryStore) {
	t.Helper()
	ctx := context.Background()
	store := repository.NewMemoryStore()
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{{
		ID: "m1", BestAsk: 0.50, BestBid: 0.49,
		OrderBook: marketmodel.OrderBook{Asks: []marketmodel.OrderBookLevel{{Price: 0.50, Size: 100}, {Price: 0.55, Size: 100}}},
	}}))
	return ctx, store
}
