package executionengine

import (
	"context"
	"fmt"

	"github.com/local/polymarket-process-service/pkg/eventbus"
	"github.com/local/polymarket-process-service/pkg/idgen"
	execmodel "github.com/local/polymarket-process-service/pkg/model/execution"
	"github.com/local/polymarket-process-service/pkg/repository"
)

type Service struct {
	store           repository.Store
	provider        ExecutionProvider
	bus             eventbus.Bus
	startingCashUSD float64
}

func NewService(store repository.Store, executionProvider ExecutionProvider) *Service {
	return &Service{store: store, provider: executionProvider}
}

func (s *Service) SetEventBus(bus eventbus.Bus) {
	s.bus = bus
}

func (s *Service) SetStartingCash(startingCashUSD float64) {
	s.startingCashUSD = startingCashUSD
}

func (s *Service) CreateFromRiskDecision(ctx context.Context, riskDecisionID string) (execmodel.OrderResult, error) {
	risk, err := s.store.GetRiskDecision(ctx, riskDecisionID)
	if err != nil {
		return execmodel.OrderResult{}, err
	}
	if !risk.Approved {
		return execmodel.OrderResult{}, fmt.Errorf("risk decision is not approved: %s", risk.RejectReason)
	}
	prob, err := s.store.GetProbabilityDecision(ctx, risk.ProbabilityDecisionID)
	if err != nil {
		return execmodel.OrderResult{}, err
	}
	order := execmodel.OrderRequest{
		RiskDecisionID: risk.ID, MarketID: risk.MarketID, Outcome: prob.Outcome, Side: prob.Side,
		LimitPrice: prob.ExecutablePrice, SizeUSD: risk.PositionSizeUSD, Reason: risk.Reason,
		IdempotencyKey: idgen.New(),
	}
	result, err := s.provider.PlaceOrder(ctx, order)
	if err == nil && s.bus != nil {
		topic := "trade.rejected"
		if result.TradeID != "" {
			topic = "trade.opened"
		}
		s.bus.Publish(ctx, eventbus.Event{Topic: topic, Data: result})
		s.publishPortfolio(ctx)
	}
	return result, err
}

func (s *Service) ClosePosition(ctx context.Context, positionID, reason string) (execmodel.OrderResult, error) {
	position, err := s.store.GetPosition(ctx, positionID)
	if err != nil {
		return execmodel.OrderResult{}, err
	}
	result, err := s.provider.ClosePosition(ctx, position, reason)
	if err == nil && s.bus != nil {
		s.bus.Publish(ctx, eventbus.Event{Topic: "trade.closed", Data: result})
		s.publishPortfolio(ctx)
	}
	return result, err
}

func (s *Service) publishPortfolio(ctx context.Context) {
	if s.bus == nil {
		return
	}
	portfolio, err := s.store.Portfolio(ctx, s.startingCashUSD)
	if err != nil {
		return
	}
	s.bus.Publish(ctx, eventbus.Event{Topic: "portfolio.updated", Data: portfolio})
}
