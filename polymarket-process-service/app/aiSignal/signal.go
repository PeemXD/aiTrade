package aisignal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/local/polymarket-process-service/pkg/config"
	"github.com/local/polymarket-process-service/pkg/eventbus"
	"github.com/local/polymarket-process-service/pkg/idgen"
	aimodel "github.com/local/polymarket-process-service/pkg/model/ai"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	newsmodel "github.com/local/polymarket-process-service/pkg/model/news"
	"github.com/local/polymarket-process-service/pkg/repository"
)

const systemPrompt = `You are an analyst for prediction markets. You do not place trades. You only analyze whether the provided news changes the probability of the provided market outcome. Be conservative. If the news is already likely priced in, return small delta. If unrelated, return neutral.

Return strict JSON only. No markdown, prose, code fences, or extra keys.

Required output JSON shape:
{
  "related": true,
  "direction": "bullish",
  "probability_delta": 0.00,
  "confidence": 0.00,
  "source_reliability": 0.00,
  "reason": "short explanation",
  "priced_in_risk": "low"
}

Validation rules:
- related must be true or false.
- direction must be exactly one of: bullish, bearish, neutral.
- probability_delta must be between -0.20 and 0.20.
- confidence must be between 0 and 1.
- source_reliability must be between 0 and 1.
- priced_in_risk must be exactly one of: low, medium, high.
- if related is false, direction must be neutral and probability_delta must be 0.`

type SignalService struct {
	cfg    config.Config
	client ChatClient
	store  repository.Store
	log    *slog.Logger
	tokens chan struct{}
	bus    eventbus.Bus
}

func NewSignalService(cfg config.Config, chat ChatClient, store repository.Store, log *slog.Logger) *SignalService {
	rate := cfg.AIRateLimitPerMinute
	if rate <= 0 {
		rate = 30
	}
	s := &SignalService{cfg: cfg, client: chat, store: store, log: log, tokens: make(chan struct{}, rate)}
	for i := 0; i < rate; i++ {
		s.tokens <- struct{}{}
	}
	go func() {
		ticker := time.NewTicker(time.Minute / time.Duration(rate))
		defer ticker.Stop()
		for range ticker.C {
			select {
			case s.tokens <- struct{}{}:
			default:
			}
		}
	}()
	return s
}

func (s *SignalService) SetEventBus(bus eventbus.Bus) {
	s.bus = bus
}

func (s *SignalService) Analyze(ctx context.Context, market marketmodel.Market, news newsmodel.NewsArticle) (aimodel.AISignal, error) {
	now := time.Now().UTC()
	if strings.TrimSpace(s.cfg.AIAPIKey) == "" {
		sig := aimodel.AISignal{
			ID: idgen.New(), MarketID: market.ID, NewsID: news.ID, Related: false, Direction: aimodel.DirectionNeutral,
			ProbabilityDelta: 0, Confidence: 0, SourceReliability: 0, PricedInRisk: "high",
			Reasoning: "AI disabled / missing key", Disabled: true, CreatedAt: now,
		}
		_ = s.store.SaveAISignal(ctx, sig)
		_ = s.store.SaveAudit(ctx, repository.AuditLog{ID: idgen.New(), Event: "ai_signal_generated", EntityID: market.ID, Payload: map[string]any{"market_id": market.ID, "disabled": true, "reason": sig.Reasoning}, CreatedAt: now})
		s.publish(ctx, sig)
		return sig, nil
	}
	select {
	case <-s.tokens:
	case <-ctx.Done():
		return aimodel.AISignal{}, ctx.Err()
	}
	input := map[string]any{
		"market_question":    market.Question,
		"market_probability": impliedProbability(market),
		"best_bid":           market.BestBid,
		"best_ask":           market.BestAsk,
		"news_title":         news.Title,
		"news_content":       news.Content,
		"news_source":        news.Source,
		"published_at":       news.PublishedAt.Format(time.RFC3339),
	}
	raw, err := s.client.ChatJSON(ctx, ChatRequest{Model: s.cfg.AIModel, System: systemPrompt, UserJSON: input, Temperature: s.cfg.AITemperature, APIKey: s.cfg.AIAPIKey})
	if err != nil {
		return aimodel.AISignal{}, err
	}
	out, err := parseAIOutput(raw)
	if err != nil {
		return aimodel.AISignal{}, err
	}
	sig := aimodel.AISignal{
		ID: idgen.New(), MarketID: market.ID, NewsID: news.ID, Related: out.Related, Direction: aimodel.Direction(out.Direction),
		ProbabilityDelta: out.ProbabilityDelta, Confidence: out.Confidence, SourceReliability: out.SourceReliability,
		PricedInRisk: out.PricedInRisk, Reasoning: out.Reason, RawResponse: raw, CreatedAt: now,
	}
	if !sig.Related {
		sig.ProbabilityDelta = 0
		sig.Direction = aimodel.DirectionNeutral
	}
	if err := s.store.SaveAISignal(ctx, sig); err != nil {
		return sig, err
	}
	_ = s.store.SaveAudit(ctx, repository.AuditLog{ID: idgen.New(), Event: "ai_signal_generated", EntityID: market.ID, Payload: map[string]any{"market_id": market.ID, "direction": sig.Direction, "delta": sig.ProbabilityDelta, "confidence": sig.Confidence}, CreatedAt: now})
	s.publish(ctx, sig)
	s.log.Info("ai_signal_generated", "market_id", market.ID, "direction", sig.Direction, "delta", sig.ProbabilityDelta, "confidence", sig.Confidence)
	return sig, nil
}

func (s *SignalService) publish(ctx context.Context, sig aimodel.AISignal) {
	if s.bus == nil {
		return
	}
	s.bus.Publish(ctx, eventbus.Event{Topic: "ai.signal.generated", Data: sig})
}

func parseAIOutput(raw string) (aimodel.AIProviderOutput, error) {
	clean := strings.TrimSpace(raw)
	clean = strings.TrimPrefix(clean, "```json")
	clean = strings.TrimPrefix(clean, "```")
	clean = strings.TrimSuffix(clean, "```")
	var out aimodel.AIProviderOutput
	if err := json.Unmarshal([]byte(strings.TrimSpace(clean)), &out); err != nil {
		return out, err
	}
	out.Direction = strings.ToLower(out.Direction)
	out.PricedInRisk = strings.ToLower(out.PricedInRisk)
	if out.Direction != "bullish" && out.Direction != "bearish" && out.Direction != "neutral" {
		return out, fmt.Errorf("invalid ai direction %q", out.Direction)
	}
	if out.PricedInRisk != "low" && out.PricedInRisk != "medium" && out.PricedInRisk != "high" {
		return out, fmt.Errorf("invalid priced_in_risk %q", out.PricedInRisk)
	}
	if strings.TrimSpace(out.Reason) == "" {
		return out, fmt.Errorf("ai reason is required")
	}
	if out.ProbabilityDelta < -0.20 || out.ProbabilityDelta > 0.20 {
		return out, fmt.Errorf("probability_delta out of range")
	}
	if out.Confidence < 0 || out.Confidence > 1 || out.SourceReliability < 0 || out.SourceReliability > 1 {
		return out, fmt.Errorf("ai confidence/reliability out of range")
	}
	if !out.Related && out.Direction != "neutral" {
		return out, fmt.Errorf("unrelated ai output must be neutral")
	}
	if !out.Related && math.Abs(out.ProbabilityDelta) > 0 {
		return out, fmt.Errorf("unrelated ai output must have zero delta")
	}
	return out, nil
}

func impliedProbability(m marketmodel.Market) float64 {
	if m.BestBid > 0 && m.BestAsk > 0 {
		return (m.BestBid + m.BestAsk) / 2
	}
	if m.YesPrice > 0 {
		return m.YesPrice
	}
	return 0.5
}
