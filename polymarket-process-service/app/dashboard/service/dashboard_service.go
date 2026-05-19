package service

import (
	"context"
	"time"

	dashboardmodel "github.com/local/polymarket-process-service/app/dashboard/model"
	"github.com/local/polymarket-process-service/pkg/config"
	aimodel "github.com/local/polymarket-process-service/pkg/model/ai"
	execmodel "github.com/local/polymarket-process-service/pkg/model/execution"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	probmodel "github.com/local/polymarket-process-service/pkg/model/probability"
	riskmodel "github.com/local/polymarket-process-service/pkg/model/risk"
	"github.com/local/polymarket-process-service/pkg/repository"
)

type BotStatusProvider interface {
	Status() string
}

type DashboardService struct {
	cfg    config.Config
	store  repository.Store
	status BotStatusProvider
}

func NewDashboardService(cfg config.Config, store repository.Store, status BotStatusProvider) *DashboardService {
	return &DashboardService{cfg: cfg, store: store, status: status}
}

func (s *DashboardService) Summary(ctx context.Context) (dashboardmodel.Summary, error) {
	portfolio, err := s.store.Portfolio(ctx, s.cfg.PaperStartingBalanceUSD)
	if err != nil {
		return dashboardmodel.Summary{}, err
	}
	markets, err := s.store.ListMarkets(ctx)
	if err != nil {
		return dashboardmodel.Summary{}, err
	}
	signals, _ := s.store.ListAISignals(ctx, "", "")
	probs, _ := s.store.ListProbabilityDecisions(ctx, 10000)
	risks, _ := s.store.ListRiskDecisions(ctx, 10000)
	trades, _ := s.store.ListTrades(ctx, 10000)
	openPositions, _ := s.store.ListPositions(ctx, "open")
	status := "stopped"
	if s.status != nil {
		status = s.status.Status()
	}
	today := time.Now().UTC()
	summary := dashboardmodel.Summary{
		BotStatus:     status,
		ExecutionMode: s.cfg.ExecutionMode,
		Kafka:         dashboardmodel.KafkaSummary{Enabled: s.cfg.KafkaEnabled, Brokers: append([]string(nil), s.cfg.KafkaBrokers...)},
		Portfolio: dashboardmodel.PortfolioSummary{
			CashUSD:          portfolio.CashUSD,
			EquityUSD:        portfolio.PortfolioValueUSD,
			RealizedPnLUSD:   portfolio.RealizedPnLUSD,
			UnrealizedPnLUSD: portfolio.OpenPnLUSD,
			OpenPositions:    portfolio.OpenPositions,
		},
		Markets: dashboardmodel.MarketSummary{Tracked: len(markets), LiveStates: countLiveStates(ctx, s.store, markets)},
		Signals: dashboardmodel.SignalSummary{Today: countSignalsToday(signals, today), Candidates: countCandidates(probs)},
		Risk:    dashboardmodel.RiskSummary{ApprovedToday: countRisks(risks, true, today), RejectedToday: countRisks(risks, false, today)},
		Trades:  dashboardmodel.TradeSummary{OpenedToday: countOpenedTrades(trades, today), ClosedToday: countClosedTrades(trades, today)},
	}
	if len(openPositions) > 0 && summary.Portfolio.OpenPositions == 0 {
		summary.Portfolio.OpenPositions = len(openPositions)
	}
	return summary, nil
}

func countLiveStates(ctx context.Context, store repository.Store, markets []marketmodel.Market) int {
	count := 0
	for _, market := range markets {
		if state, err := store.GetLiveMarketState(ctx, market.ID); err == nil && state.MarketID != "" {
			count++
		}
	}
	return count
}

func countSignalsToday(items []aimodel.AISignal, today time.Time) int {
	count := 0
	for _, item := range items {
		if sameUTCDate(item.CreatedAt, today) {
			count++
		}
	}
	return count
}

func countCandidates(items []probmodel.ProbabilityDecision) int {
	count := 0
	for _, item := range items {
		if item.Decision == probmodel.DecisionCandidate {
			count++
		}
	}
	return count
}

func countRisks(items []riskmodel.RiskDecision, approved bool, today time.Time) int {
	count := 0
	for _, item := range items {
		if item.Approved == approved && sameUTCDate(item.CreatedAt, today) {
			count++
		}
	}
	return count
}

func countOpenedTrades(items []execmodel.Trade, today time.Time) int {
	count := 0
	for _, item := range items {
		if item.OpenedAt != nil && sameUTCDate(*item.OpenedAt, today) {
			count++
		}
	}
	return count
}

func countClosedTrades(items []execmodel.Trade, today time.Time) int {
	count := 0
	for _, item := range items {
		if item.ClosedAt != nil && sameUTCDate(*item.ClosedAt, today) {
			count++
		}
	}
	return count
}

func sameUTCDate(left, right time.Time) bool {
	ly, lm, ld := left.UTC().Date()
	ry, rm, rd := right.UTC().Date()
	return ly == ry && lm == rm && ld == rd
}
