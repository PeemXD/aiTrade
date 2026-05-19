package router

import (
	"net/http"
	"strings"

	dashboardhandler "github.com/local/polymarket-process-service/app/dashboard/handler"
	livehandler "github.com/local/polymarket-process-service/app/live/handler"
)

func New(dashboard *dashboardhandler.DashboardHandler, sse *dashboardhandler.SSEHandler, live *livehandler.LiveHandler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", withMethod(http.MethodGet, dashboard.Health))
	mux.HandleFunc("/api/v1/dashboard/summary", withMethod(http.MethodGet, dashboard.Summary))
	mux.HandleFunc("/api/v1/dashboard/stream", withMethod(http.MethodGet, sse.Stream))
	mux.HandleFunc("/api/v1/markets", withMethod(http.MethodGet, dashboard.Markets))
	mux.HandleFunc("/api/v1/news", withMethod(http.MethodGet, dashboard.News))
	mux.HandleFunc("/api/v1/signals", withMethod(http.MethodGet, dashboard.Signals))
	mux.HandleFunc("/api/v1/probability-decisions", withMethod(http.MethodGet, dashboard.ProbabilityDecisions))
	mux.HandleFunc("/api/v1/risk-decisions", withMethod(http.MethodGet, dashboard.RiskDecisions))
	mux.HandleFunc("/api/v1/trades", withMethod(http.MethodGet, dashboard.Trades))
	mux.HandleFunc("/api/v1/positions", withMethod(http.MethodGet, dashboard.Positions))
	mux.HandleFunc("/api/v1/portfolio", withMethod(http.MethodGet, dashboard.Portfolio))
	mux.HandleFunc("/api/v1/audit", withMethod(http.MethodGet, dashboard.Audit))
	mux.HandleFunc("/api/v1/live/run-once", withMethod(http.MethodPost, live.RunOnce))
	mux.HandleFunc("/api/v1/live/start", withMethod(http.MethodPost, live.Start))
	mux.HandleFunc("/api/v1/live/stop", withMethod(http.MethodPost, live.Stop))
	mux.HandleFunc("/api/v1/positions/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/close") {
			live.ClosePosition(w, r)
			return
		}
		http.NotFound(w, r)
	})
	return mux
}
