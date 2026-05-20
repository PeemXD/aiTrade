package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	aisignal "github.com/local/polymarket-process-service/app/aiSignal"
	executionengine "github.com/local/polymarket-process-service/app/executionEngine"
	exitengine "github.com/local/polymarket-process-service/app/exitEngine"
	livemodel "github.com/local/polymarket-process-service/app/live/model"
	newsmarketmatcher "github.com/local/polymarket-process-service/app/newsMarketMatcher"
	positionengine "github.com/local/polymarket-process-service/app/positionEngine"
	probabilityengine "github.com/local/polymarket-process-service/app/probabilityEngine"
	riskengine "github.com/local/polymarket-process-service/app/riskEngine"
	"github.com/local/polymarket-process-service/pkg/config"
	"github.com/local/polymarket-process-service/pkg/eventbus"
	"github.com/local/polymarket-process-service/pkg/idgen"
	"github.com/local/polymarket-process-service/pkg/kafka"
	aimodel "github.com/local/polymarket-process-service/pkg/model/ai"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	matchmodel "github.com/local/polymarket-process-service/pkg/model/match"
	newsmodel "github.com/local/polymarket-process-service/pkg/model/news"
	positionmodel "github.com/local/polymarket-process-service/pkg/model/position"
	probmodel "github.com/local/polymarket-process-service/pkg/model/probability"
	riskmodel "github.com/local/polymarket-process-service/pkg/model/risk"
	"github.com/local/polymarket-process-service/pkg/repository"
)

const maxSeenArticles = 1000

type Service struct {
	cfg       config.Config
	store     repository.Store
	matcher   *newsmarketmatcher.MatcherService
	ai        *aisignal.SignalService
	prob      *probabilityengine.Engine
	risk      *riskengine.Engine
	execution *executionengine.Service
	monitor   *positionengine.Monitor
	exit      *exitengine.ExitEngine
	bus       eventbus.Bus
	log       *slog.Logger
	seen      *seenTracker
	loopMu    sync.Mutex
	runMu     sync.Mutex
	running   bool
	stopping  bool
	cancel    context.CancelFunc
}

type Dependencies struct {
	Config    config.Config
	Store     repository.Store
	Matcher   *newsmarketmatcher.MatcherService
	AI        *aisignal.SignalService
	Prob      *probabilityengine.Engine
	Risk      *riskengine.Engine
	Execution *executionengine.Service
	Monitor   *positionengine.Monitor
	Exit      *exitengine.ExitEngine
	Bus       eventbus.Bus
	Log       *slog.Logger
}

func NewService(deps Dependencies) *Service {
	if deps.Log == nil {
		deps.Log = slog.Default()
	}
	return &Service{
		cfg: deps.Config, store: deps.Store, matcher: deps.Matcher, ai: deps.AI, prob: deps.Prob,
		risk: deps.Risk, execution: deps.Execution, monitor: deps.Monitor, exit: deps.Exit,
		bus: deps.Bus, log: deps.Log, seen: newSeenTracker(maxSeenArticles),
	}
}

func (s *Service) RunOnce(ctx context.Context) (result livemodel.LiveResult, err error) {
	if !s.runMu.TryLock() {
		s.recordError(ctx, &result, "run_once_skipped", "previous RunOnce is still running", nil)
		return result, nil
	}
	defer s.runMu.Unlock()
	defer func() {
		if recovered := recover(); recovered != nil {
			s.recordError(ctx, &result, "run_once_panic_recovered", "RunOnce panic recovered", nil)
			s.log.Error("live_run_once_panic_recovered", "panic", recovered)
			err = nil
		}
	}()
	if !s.cfg.LiveRunOncePublishPipelineEvents {
		ctx = eventbus.WithoutPipelineEvents(ctx)
	}
	markets, err := s.store.ListMarkets(ctx)
	if err != nil {
		s.recordError(ctx, &result, "load_markets_failed", "failed to load markets", err)
		return result, nil
	}
	result.MarketsLoaded = len(markets)
	articles, err := s.store.ListNewsArticles(ctx, 100)
	if err != nil {
		s.recordError(ctx, &result, "load_news_failed", "failed to load news", err)
		return result, nil
	}
	result.ArticlesLoaded = len(articles)
	for _, article := range articles {
		if s.seen.Has(article.ID) {
			continue
		}
		s.processArticle(ctx, article, markets, &result)
		s.seen.Add(article.ID)
	}
	s.monitorPositions(ctx, &result)
	s.publishAudit(ctx, "live_run_once_completed", "", map[string]any{
		"markets_loaded":         result.MarketsLoaded,
		"articles_loaded":        result.ArticlesLoaded,
		"matches_created":        result.MatchesCreated,
		"ai_calls":               result.AICalls,
		"probability_candidates": result.ProbabilityCandidates,
		"risk_approved":          result.RiskApproved,
		"risk_rejected":          result.RiskRejected,
		"paper_trades_opened":    result.PaperTradesOpened,
		"positions_closed":       result.PositionsClosed,
	})
	return result, nil
}

func (s *Service) processArticle(ctx context.Context, article newsmodel.NewsArticle, markets []marketmodel.Market, result *livemodel.LiveResult) {
	matches := s.matcher.Match(article, markets)
	if len(matches) == 0 {
		s.publishAudit(ctx, "news_ignored", article.ID, map[string]any{"reason": "no news-market matches above threshold", "title": article.Title})
		return
	}
	for _, match := range matches {
		result.MatchesCreated++
		if err := s.store.SaveMatch(ctx, match); err != nil {
			s.recordError(ctx, result, "save_match_failed", "failed to save news-market match", err)
			continue
		}
		s.publishAudit(ctx, "news_matched", match.MarketID, map[string]any{"news_id": match.NewsID, "market_id": match.MarketID, "score": match.FinalScore, "reason": match.Reason})
		s.publish(ctx, kafka.TopicNewsMarketMatched, match)
		market, err := s.store.GetMarket(ctx, match.MarketID)
		if err != nil {
			s.recordError(ctx, result, "market_not_found", "matched market was not found", err)
			continue
		}
		signal, err := s.ai.Analyze(ctx, market, article)
		if err != nil {
			s.recordError(ctx, result, "ai_signal_failed", "AI signal generation failed", err)
			continue
		}
		if !signal.Disabled {
			result.AICalls++
		}
		decision, err := s.prob.Calculate(ctx, probmodel.CalculateRequest{MarketID: market.ID, NewsID: article.ID, Side: "buy", Outcome: "yes"})
		if err != nil {
			s.recordError(ctx, result, "probability_failed", "probability calculation failed", err)
			continue
		}
		if decision.Decision != probmodel.DecisionCandidate {
			continue
		}
		result.ProbabilityCandidates++
		risk, err := s.risk.Evaluate(ctx, riskmodel.EvaluateRequest{ProbabilityDecisionID: decision.ID})
		if err != nil {
			s.recordError(ctx, result, "risk_failed", "risk evaluation failed", err)
			continue
		}
		if !risk.Approved {
			result.RiskRejected++
			continue
		}
		result.RiskApproved++
		order, err := s.execution.CreateFromRiskDecision(ctx, risk.ID)
		if err != nil {
			s.recordError(ctx, result, "execution_failed", "paper execution failed", err)
			continue
		}
		if order.TradeID != "" {
			result.PaperTradesOpened++
		}
	}
}

func (s *Service) monitorPositions(ctx context.Context, result *livemodel.LiveResult) {
	decisions, err := s.monitor.UpdateOpenPositions(ctx)
	if err != nil {
		s.recordError(ctx, result, "position_monitor_failed", "position monitor failed", err)
		return
	}
	for _, decision := range decisions {
		if !decision.ShouldExit || decision.PositionID == "" {
			continue
		}
		if _, err := s.exit.Execute(ctx, decision); err != nil {
			s.recordError(ctx, result, "exit_position_failed", "exit position failed", err)
			continue
		}
		result.PositionsClosed++
	}
}

func (s *Service) HandleEnvelope(ctx context.Context, envelope kafka.EventEnvelope) error {
	switch envelope.EventType {
	case kafka.TopicMarketSelected:
		var market marketmodel.Market
		if err := unmarshalPayload(envelope, &market); err != nil {
			return err
		}
		if err := s.store.SaveMarkets(ctx, []marketmodel.Market{market}); err != nil {
			return err
		}
		s.publishAudit(ctx, "market_selected", market.ID, map[string]any{"market_id": market.ID, "source_event_id": envelope.EventID})
	case kafka.TopicNewsArrived:
		var article newsmodel.NewsArticle
		if err := unmarshalPayload(envelope, &article); err != nil {
			return err
		}
		return s.HandleNewsArrived(ctx, article)
	case kafka.TopicNewsMarketMatched:
		var match matchmodel.NewsMarketMatch
		if err := unmarshalPayload(envelope, &match); err != nil {
			return err
		}
		return s.HandleNewsMarketMatched(ctx, match)
	case kafka.TopicAISignalGenerated:
		var signal aimodel.AISignal
		if err := unmarshalPayload(envelope, &signal); err != nil {
			return err
		}
		return s.HandleAISignalGenerated(ctx, signal)
	case kafka.TopicProbabilityCandidate:
		var decision probmodel.ProbabilityDecision
		if err := unmarshalPayload(envelope, &decision); err != nil {
			return err
		}
		return s.HandleProbabilityCandidate(ctx, decision)
	case kafka.TopicRiskApproved:
		var decision riskmodel.RiskDecision
		if err := unmarshalPayload(envelope, &decision); err != nil {
			return err
		}
		return s.HandleRiskApproved(ctx, decision)
	case kafka.TopicMarketPriceUpdated, kafka.TopicMarketOrderBookUpdated:
		var state marketmodel.LiveMarketState
		if err := unmarshalPayload(envelope, &state); err != nil {
			return err
		}
		return s.HandleMarketStateUpdated(ctx, state, envelope.EventType)
	case kafka.TopicPositionExitCandidate:
		var decision positionmodel.ExitDecision
		if err := unmarshalPayload(envelope, &decision); err != nil {
			return err
		}
		return s.HandlePositionExitCandidate(ctx, decision)
	}
	return nil
}

func (s *Service) HandleNewsArrived(ctx context.Context, article newsmodel.NewsArticle) error {
	if err := s.store.SaveNewsArticles(ctx, []newsmodel.NewsArticle{article}); err != nil {
		return err
	}
	markets, err := s.store.ListMarkets(ctx)
	if err != nil {
		return err
	}
	matches := s.matcher.Match(article, markets)
	if len(matches) == 0 {
		s.publishAudit(ctx, "news_ignored", article.ID, map[string]any{"reason": "no news-market matches above threshold", "title": article.Title})
		return nil
	}
	for _, match := range matches {
		if err := s.store.SaveMatch(ctx, match); err != nil {
			return err
		}
		s.publishAudit(ctx, "news_matched", match.MarketID, map[string]any{"news_id": match.NewsID, "market_id": match.MarketID, "score": match.FinalScore, "reason": match.Reason})
		s.publish(ctx, kafka.TopicNewsMarketMatched, match)
	}
	return nil
}

func (s *Service) HandleNewsMarketMatched(ctx context.Context, match matchmodel.NewsMarketMatch) error {
	if err := s.store.SaveMatch(ctx, match); err != nil {
		return err
	}
	market, err := s.store.GetMarket(ctx, match.MarketID)
	if err != nil {
		return err
	}
	article, err := s.store.GetNewsArticle(ctx, match.NewsID)
	if err != nil {
		return err
	}
	_, err = s.ai.Analyze(ctx, market, article)
	return err
}

func (s *Service) HandleAISignalGenerated(ctx context.Context, signal aimodel.AISignal) error {
	if err := s.store.SaveAISignal(ctx, signal); err != nil {
		return err
	}
	_, err := s.prob.Calculate(ctx, probmodel.CalculateRequest{MarketID: signal.MarketID, NewsID: signal.NewsID, Side: "buy", Outcome: "yes"})
	return err
}

func (s *Service) HandleProbabilityCandidate(ctx context.Context, decision probmodel.ProbabilityDecision) error {
	if err := s.store.SaveProbabilityDecision(ctx, decision); err != nil {
		return err
	}
	_, err := s.risk.Evaluate(ctx, riskmodel.EvaluateRequest{ProbabilityDecisionID: decision.ID})
	return err
}

func (s *Service) HandleRiskApproved(ctx context.Context, decision riskmodel.RiskDecision) error {
	if err := s.store.SaveRiskDecision(ctx, decision); err != nil {
		return err
	}
	_, err := s.execution.CreateFromRiskDecision(ctx, decision.ID)
	return err
}

func (s *Service) HandleMarketStateUpdated(ctx context.Context, state marketmodel.LiveMarketState, eventType string) error {
	if err := s.store.SaveLiveMarketState(ctx, state); err != nil {
		return err
	}
	s.publishAudit(ctx, "market_state_updated", state.MarketID, map[string]any{"event_type": eventType, "asset_id": state.AssetID})
	var result livemodel.LiveResult
	s.monitorPositions(ctx, &result)
	return nil
}

func (s *Service) HandlePositionExitCandidate(ctx context.Context, decision positionmodel.ExitDecision) error {
	_, err := s.exit.Execute(ctx, decision)
	return err
}

func (s *Service) ClosePosition(ctx context.Context, positionID string) error {
	_, err := s.exit.Execute(ctx, positionmodel.ExitDecision{ShouldExit: true, PositionID: positionID, Reason: "manual_close"})
	return err
}

func (s *Service) publish(ctx context.Context, topic string, data any) {
	if s.bus != nil {
		s.bus.Publish(ctx, eventbus.Event{Topic: topic, Data: data})
	}
}

func (s *Service) recordError(ctx context.Context, result *livemodel.LiveResult, eventType, message string, err error) {
	event := liveError(eventType, message, err)
	if result != nil {
		result.Events = append(result.Events, event)
	}
	attrs := []any{"event", eventType, "message", message}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	s.log.Warn("live_run_error", attrs...)
	payload := map[string]any{"event": eventType, "message": message}
	if err != nil {
		payload["error"] = err.Error()
	}
	s.publish(ctx, "error", payload)
}

func (s *Service) publishAudit(ctx context.Context, event, entityID string, payload map[string]any) {
	_ = s.store.SaveAudit(ctx, repository.AuditLog{ID: idgen.New(), Event: event, EntityID: entityID, Payload: payload, CreatedAt: time.Now().UTC()})
	s.publish(ctx, kafka.TopicAuditCreated, map[string]any{"event": event, "entity_id": entityID, "payload": payload})
}

func unmarshalPayload(envelope kafka.EventEnvelope, out any) error {
	return json.Unmarshal(envelope.Payload, out)
}

func liveError(event, message string, err error) livemodel.LiveAuditEvent {
	if err != nil {
		if message == "" {
			message = err.Error()
		}
	}
	payload := map[string]any{}
	if err != nil {
		payload["error"] = err.Error()
	}
	return livemodel.LiveAuditEvent{Type: event, Message: message, Payload: payload}
}
