package positionengine

import (
	"context"
	"testing"
	"time"

	executionengine "github.com/local/polymarket-process-service/app/executionEngine"
	exitengine "github.com/local/polymarket-process-service/app/exitEngine"
	"github.com/local/polymarket-process-service/pkg/config"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	positionmodel "github.com/local/polymarket-process-service/pkg/model/position"
	probmodel "github.com/local/polymarket-process-service/pkg/model/probability"
	"github.com/local/polymarket-process-service/pkg/repository"
	"github.com/stretchr/testify/require"
)

func TestPositionMonitorExitWhenEdgeGone(t *testing.T) {
	monitor := NewMonitor(monitorConfig(), nil)
	position := positionmodel.Position{ExposureUSD: 100, UnrealizedPnLUSD: 0}

	decision := monitor.Evaluate(position, 0.01, false, time.Now().Add(24*time.Hour))

	require.True(t, decision.ShouldExit)
	require.Equal(t, "edge_gone", decision.Reason)
}

func TestPositionMonitorExitOnTakeProfit(t *testing.T) {
	monitor := NewMonitor(monitorConfig(), nil)
	position := positionmodel.Position{ExposureUSD: 100, UnrealizedPnLUSD: 25}

	decision := monitor.Evaluate(position, 0.10, false, time.Now().Add(24*time.Hour))

	require.True(t, decision.ShouldExit)
	require.Equal(t, "take_profit", decision.Reason)
}

func TestPositionMonitorExitOnStopLoss(t *testing.T) {
	monitor := NewMonitor(monitorConfig(), nil)
	position := positionmodel.Position{ExposureUSD: 100, UnrealizedPnLUSD: -15}

	decision := monitor.Evaluate(position, 0.10, false, time.Now().Add(24*time.Hour))

	require.True(t, decision.ShouldExit)
	require.Equal(t, "stop_loss", decision.Reason)
}

func TestPositionMonitorExitOnRiskExceeded(t *testing.T) {
	monitor := NewMonitor(monitorConfig(), nil)
	position := positionmodel.Position{ExposureUSD: 100, UnrealizedPnLUSD: 0}

	decision := monitor.Evaluate(position, 0.10, true, time.Now().Add(24*time.Hour))

	require.True(t, decision.ShouldExit)
	require.Equal(t, "risk_exceeded", decision.Reason)
}

func TestUpdateOpenPositionsUsesLatestProbabilityEdge(t *testing.T) {
	ctx := context.Background()
	store := repository.NewMemoryStore()
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{{ID: "m1", BestBid: 0.50, YesPrice: 0.50, EndTime: time.Now().Add(24 * time.Hour)}}))
	require.NoError(t, store.SavePosition(ctx, positionmodel.Position{ID: "p1", MarketID: "m1", Outcome: "yes", Quantity: 100, AvgEntryPrice: 0.50, CurrentPrice: 0.50, ExposureUSD: 50, Status: "open", OpenedAt: time.Now(), UpdatedAt: time.Now()}))
	require.NoError(t, store.SaveProbabilityDecision(ctx, probmodel.ProbabilityDecision{ID: "pd1", MarketID: "m1", Edge: 0.01, CreatedAt: time.Now()}))

	decisions, err := NewMonitor(monitorConfig(), store).UpdateOpenPositions(ctx)

	require.NoError(t, err)
	require.Len(t, decisions, 1)
	require.True(t, decisions[0].ShouldExit)
	require.Equal(t, "edge_gone", decisions[0].Reason)
	require.Equal(t, "p1", decisions[0].PositionID)
}

func TestExitEngineClosesExitCandidate(t *testing.T) {
	ctx := context.Background()
	store := repository.NewMemoryStore()
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{{ID: "m1", BestBid: 0.60, YesPrice: 0.60}}))
	require.NoError(t, store.SavePosition(ctx, positionmodel.Position{ID: "p1", MarketID: "m1", Outcome: "yes", Quantity: 100, AvgEntryPrice: 0.50, CurrentPrice: 0.50, ExposureUSD: 50, Status: "open", OpenedAt: time.Now(), UpdatedAt: time.Now()}))
	execution := executionengine.NewService(store, executionengine.NewPaperExecutionProvider(store))

	result, err := exitengine.NewExitEngine(store, execution).Execute(ctx, positionmodel.ExitDecision{ShouldExit: true, PositionID: "p1", MarketID: "m1", Reason: "edge_gone"})

	require.NoError(t, err)
	require.Equal(t, "closed", result.Status)
	position, err := store.GetPosition(ctx, "p1")
	require.NoError(t, err)
	require.Equal(t, "closed", position.Status)
}

func TestExitEngineRejectsInvalidExitReason(t *testing.T) {
	store := repository.NewMemoryStore()
	execution := executionengine.NewService(store, executionengine.NewPaperExecutionProvider(store))

	_, err := exitengine.NewExitEngine(store, execution).Execute(context.Background(), positionmodel.ExitDecision{ShouldExit: true, PositionID: "p1", Reason: "bad_reason"})

	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid exit reason")
}

func monitorConfig() config.Config {
	return config.Config{EdgeThreshold: 0.05, TakeProfitPct: 0.20, StopLossPct: 0.10, ExitBeforeExpiryHours: 6}
}
