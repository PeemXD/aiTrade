package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	aisignal "github.com/local/polymarket-process-service/app/aiSignal"
	executionengine "github.com/local/polymarket-process-service/app/executionEngine"
	exitengine "github.com/local/polymarket-process-service/app/exitEngine"
	newsmarketmatcher "github.com/local/polymarket-process-service/app/newsMarketMatcher"
	positionengine "github.com/local/polymarket-process-service/app/positionEngine"
	probabilityengine "github.com/local/polymarket-process-service/app/probabilityEngine"
	riskengine "github.com/local/polymarket-process-service/app/riskEngine"
	"github.com/local/polymarket-process-service/pkg/config"
	"github.com/local/polymarket-process-service/pkg/eventbus"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	newsmodel "github.com/local/polymarket-process-service/pkg/model/news"
	"github.com/local/polymarket-process-service/pkg/repository"
	"github.com/stretchr/testify/require"
)

func TestRunOnceRunsFullCycleWithMockedAI(t *testing.T) {
	store := repository.NewMemoryStore()
	ctx := context.Background()
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{testMarket()}))
	require.NoError(t, store.SaveNewsArticles(ctx, []newsmodel.NewsArticle{testArticle()}))
	service := newTestLiveService(t, store, testConfig("test-key"), fakeChatClient{})
	result, err := service.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.MarketsLoaded)
	require.Equal(t, 1, result.ArticlesLoaded)
	require.Equal(t, 1, result.MatchesCreated)
	require.Equal(t, 1, result.AICalls)
	require.Equal(t, 1, result.ProbabilityCandidates)
	require.Equal(t, 1, result.RiskApproved)
	require.Equal(t, 1, result.PaperTradesOpened)
}

func TestRunOnceMissingAIKeyDoesNotCrash(t *testing.T) {
	store := repository.NewMemoryStore()
	ctx := context.Background()
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{testMarket()}))
	require.NoError(t, store.SaveNewsArticles(ctx, []newsmodel.NewsArticle{testArticle()}))
	service := newTestLiveService(t, store, testConfig(""), fakeChatClient{})
	result, err := service.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.MatchesCreated)
	require.Zero(t, result.AICalls)
	require.Zero(t, result.PaperTradesOpened)
}

func TestRunOnceExternalErrorReturnsGracefulResult(t *testing.T) {
	service := newTestLiveService(t, failingStore{Store: repository.NewMemoryStore()}, testConfig("test-key"), fakeChatClient{})
	result, err := service.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, result.Events, 1)
	require.Equal(t, "load_markets_failed", result.Events[0].Type)
}

func TestRunOnceDoesNotOverlapAndPublishesErrorEvent(t *testing.T) {
	store := repository.NewMemoryStore()
	bus := &recordingBus{}
	service := newTestLiveServiceWithBus(t, store, testConfig("test-key"), fakeChatClient{}, bus)
	service.runMu.Lock()
	result, err := service.RunOnce(context.Background())
	service.runMu.Unlock()
	require.NoError(t, err)
	require.Len(t, result.Events, 1)
	require.Equal(t, "run_once_skipped", result.Events[0].Type)
	require.True(t, bus.HasTopic("error"))
}

func TestLiveLoopStartStopAreIdempotent(t *testing.T) {
	store := repository.NewMemoryStore()
	cfg := testConfig("")
	cfg.LiveLoopInterval = time.Hour
	service := newTestLiveService(t, store, cfg, fakeChatClient{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.True(t, service.Start(ctx))
	require.False(t, service.Start(ctx))
	require.Equal(t, "running", service.Status())
	require.True(t, service.Stop())
	require.False(t, service.Stop())
	require.Eventually(t, func() bool {
		return service.Status() == "stopped"
	}, time.Second, 10*time.Millisecond)
}

func newTestLiveService(t *testing.T, store repository.Store, cfg config.Config, chat aisignal.ChatClient) *Service {
	t.Helper()
	return newTestLiveServiceWithBus(t, store, cfg, chat, eventbus.NewInMemory())
}

func newTestLiveServiceWithBus(t *testing.T, store repository.Store, cfg config.Config, chat aisignal.ChatClient, bus eventbus.Bus) *Service {
	t.Helper()
	matcher := newsmarketmatcher.NewMatcherService(0.01, 3)
	ai := aisignal.NewSignalService(cfg, chat, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	prob := probabilityengine.NewEngine(cfg, store)
	risk := riskengine.NewEngine(cfg, store)
	execution := executionengine.NewService(store, executionengine.NewPaperExecutionProvider(store))
	execution.SetStartingCash(cfg.PaperStartingBalanceUSD)
	monitor := positionengine.NewMonitor(cfg, store)
	exit := exitengine.NewExitEngine(store, execution)
	ai.SetEventBus(bus)
	prob.SetEventBus(bus)
	risk.SetEventBus(bus)
	execution.SetEventBus(bus)
	monitor.SetEventBus(bus)
	exit.SetEventBus(bus)
	return NewService(Dependencies{
		Config: cfg, Store: store, Matcher: matcher, AI: ai, Prob: prob, Risk: risk,
		Execution: execution, Monitor: monitor, Exit: exit, Bus: bus,
	})
}

func testConfig(apiKey string) config.Config {
	return config.Config{
		AIAPIKey:                apiKey,
		AIModel:                 "test-model",
		AIRateLimitPerMinute:    100,
		PaperStartingBalanceUSD: 10000,
		ExecutionMode:           "paper",
		EdgeThreshold:           0.05,
		MinSignalConfidence:     0.5,
		MarketMaxSpread:         0.10,
		MarketMinLiquidityUSD:   1000,
		MarketMinHoursToExpiry:  1,
		MaxDailyLossPct:         0.03,
		MaxTotalOpenPositions:   10,
		MaxPositionPerMarketPct: 0.10,
		MaxCategoryExposurePct:  0.50,
		MaxOrderbookImpactPct:   1.0,
		KellyFraction:           0.25,
		TakeProfitPct:           0.20,
		StopLossPct:             0.10,
		ExitBeforeExpiryHours:   6,
		LiveLoopInterval:        time.Minute,
	}
}

func testMarket() marketmodel.Market {
	return marketmodel.Market{
		ID:        "market-1",
		Question:  "Will bitcoin ETF be approved by SEC?",
		Category:  "crypto",
		Active:    true,
		EndTime:   time.Now().UTC().Add(24 * time.Hour),
		Volume:    100000,
		Liquidity: 100000,
		YesPrice:  0.395,
		BestBid:   0.39,
		BestAsk:   0.40,
		Spread:    0.01,
		OrderBook: marketmodel.OrderBook{Asks: []marketmodel.OrderBookLevel{{Price: 0.40, Size: 100000}}},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
}

func testArticle() newsmodel.NewsArticle {
	now := time.Now().UTC()
	return newsmodel.NewsArticle{
		ID: "news-1", Source: "mock", Title: "SEC approves bitcoin ETF",
		Content:     "The SEC approved a bitcoin ETF after market close.",
		PublishedAt: now, FetchedAt: now, Hash: "news-1",
	}
}

type fakeChatClient struct{}

func (fakeChatClient) ChatJSON(context.Context, aisignal.ChatRequest) (string, error) {
	return `{"related":true,"direction":"bullish","probability_delta":0.20,"confidence":1,"source_reliability":1,"reason":"directly affects the market","priced_in_risk":"low"}`, nil
}

type failingStore struct {
	repository.Store
}

func (s failingStore) ListMarkets(context.Context) ([]marketmodel.Market, error) {
	return nil, errors.New("store unavailable")
}

type recordingBus struct {
	mu     sync.Mutex
	events []eventbus.Event
}

func (b *recordingBus) Publish(_ context.Context, event eventbus.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, event)
}

func (b *recordingBus) Subscribe(string, eventbus.Handler) {}

func (b *recordingBus) HasTopic(topic string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, event := range b.events {
		if event.Topic == topic {
			return true
		}
	}
	return false
}
