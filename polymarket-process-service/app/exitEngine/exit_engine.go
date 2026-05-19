package exitengine

import (
	"context"
	"fmt"
	"time"

	executionengine "github.com/local/polymarket-process-service/app/executionEngine"
	"github.com/local/polymarket-process-service/pkg/eventbus"
	"github.com/local/polymarket-process-service/pkg/idgen"
	execmodel "github.com/local/polymarket-process-service/pkg/model/execution"
	positionmodel "github.com/local/polymarket-process-service/pkg/model/position"
	"github.com/local/polymarket-process-service/pkg/repository"
)

type ExitEngine struct {
	store     repository.Store
	execution *executionengine.Service
	bus       eventbus.Bus
}

func NewExitEngine(store repository.Store, execution *executionengine.Service) *ExitEngine {
	return &ExitEngine{store: store, execution: execution}
}

func (e *ExitEngine) SetEventBus(bus eventbus.Bus) {
	e.bus = bus
}

func (e *ExitEngine) Execute(ctx context.Context, decision positionmodel.ExitDecision) (execmodel.OrderResult, error) {
	if !decision.ShouldExit {
		return execmodel.OrderResult{}, fmt.Errorf("exit decision is not an exit candidate")
	}
	if decision.PositionID == "" {
		return execmodel.OrderResult{}, fmt.Errorf("position_id is required")
	}
	if !validExitReason(decision.Reason) {
		return execmodel.OrderResult{}, fmt.Errorf("invalid exit reason %q", decision.Reason)
	}
	result, err := e.execution.ClosePosition(ctx, decision.PositionID, decision.Reason)
	if err != nil {
		return result, err
	}
	_ = e.store.SaveAudit(ctx, repository.AuditLog{
		ID:       idgen.New(),
		Event:    "exit_executed",
		EntityID: decision.PositionID,
		Payload: map[string]any{
			"market_id": decision.MarketID,
			"reason":    decision.Reason,
			"trade_id":  result.TradeID,
		},
		CreatedAt: time.Now().UTC(),
	})
	if e.bus != nil {
		e.bus.Publish(ctx, eventbus.Event{Topic: "position.exit_executed", Data: result})
	}
	return result, nil
}

func validExitReason(reason string) bool {
	switch reason {
	case "edge_gone", "take_profit", "stop_loss", "risk_exceeded", "expiry_close", "manual_close":
		return true
	default:
		return false
	}
}
