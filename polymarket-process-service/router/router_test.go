package router

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	aisignal "github.com/local/polymarket-process-service/app/aiSignal"
	dashboardhandler "github.com/local/polymarket-process-service/app/dashboard/handler"
	dashboardservice "github.com/local/polymarket-process-service/app/dashboard/service"
	executionengine "github.com/local/polymarket-process-service/app/executionEngine"
	exitengine "github.com/local/polymarket-process-service/app/exitEngine"
	livehandler "github.com/local/polymarket-process-service/app/live/handler"
	liveservice "github.com/local/polymarket-process-service/app/live/service"
	newsmarketmatcher "github.com/local/polymarket-process-service/app/newsMarketMatcher"
	positionengine "github.com/local/polymarket-process-service/app/positionEngine"
	probabilityengine "github.com/local/polymarket-process-service/app/probabilityEngine"
	riskengine "github.com/local/polymarket-process-service/app/riskEngine"
	"github.com/local/polymarket-process-service/pkg/config"
	"github.com/local/polymarket-process-service/pkg/eventbus"
	"github.com/local/polymarket-process-service/pkg/repository"
	"github.com/stretchr/testify/require"
)

func TestHandlersHealthSummaryAndRunOnce(t *testing.T) {
	handler := newTestRouter()
	tests := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/health"},
		{http.MethodGet, "/api/v1/dashboard/summary"},
		{http.MethodPost, "/api/v1/live/run-once"},
	}
	for _, tt := range tests {
		req := httptest.NewRequest(tt.method, tt.path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, tt.path)
	}
}

func newTestRouter() http.Handler {
	cfg := config.Config{
		ExecutionMode:           "paper",
		PaperStartingBalanceUSD: 10000,
		AIRateLimitPerMinute:    100,
		DashboardSSEHeartbeat:   time.Second,
		DashboardMarketThrottle: time.Second,
	}
	store := repository.NewMemoryStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	stream := dashboardservice.NewStreamService(cfg.DashboardMarketThrottle)
	bus := eventbus.NewComposite(eventbus.NewInMemory(), dashboardservice.NewEventBusStream(stream))
	chat := noopChat{}
	ai := aisignal.NewSignalService(cfg, chat, store, log)
	prob := probabilityengine.NewEngine(cfg, store)
	risk := riskengine.NewEngine(cfg, store)
	execution := executionengine.NewService(store, executionengine.NewPaperExecutionProvider(store))
	execution.SetStartingCash(cfg.PaperStartingBalanceUSD)
	monitor := positionengine.NewMonitor(cfg, store)
	exit := exitengine.NewExitEngine(store, execution)
	for _, setter := range []interface{ SetEventBus(eventbus.Bus) }{ai, prob, risk, execution, monitor, exit} {
		setter.SetEventBus(bus)
	}
	live := liveservice.NewService(liveservice.Dependencies{
		Config: cfg, Store: store, Matcher: newsmarketmatcher.NewMatcherService(0.01, 3),
		AI: ai, Prob: prob, Risk: risk, Execution: execution, Monitor: monitor, Exit: exit, Bus: bus, Log: log,
	})
	dashboardSvc := dashboardservice.NewDashboardService(cfg, store, live)
	dashboardHandler := dashboardhandler.NewDashboardHandler(cfg, store, dashboardSvc)
	sseHandler := dashboardhandler.NewSSEHandler(stream, cfg.DashboardSSEHeartbeat)
	liveHandler := livehandler.NewLiveHandler(live)
	return New(dashboardHandler, sseHandler, liveHandler)
}

type noopChat struct{}

func (noopChat) ChatJSON(context.Context, aisignal.ChatRequest) (string, error) {
	return `{}`, nil
}
