package handler

import (
	"net/http"

	dashboardservice "github.com/local/polymarket-process-service/app/dashboard/service"
	"github.com/local/polymarket-process-service/pkg/config"
	"github.com/local/polymarket-process-service/pkg/repository"
	"github.com/local/polymarket-process-service/pkg/response"
)

type DashboardHandler struct {
	cfg       config.Config
	store     repository.Store
	dashboard *dashboardservice.DashboardService
}

func NewDashboardHandler(cfg config.Config, store repository.Store, dashboard *dashboardservice.DashboardService) *DashboardHandler {
	return &DashboardHandler{cfg: cfg, store: store, dashboard: dashboard}
}

func (h *DashboardHandler) Health(w http.ResponseWriter, _ *http.Request) {
	response.JSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"execution_mode": h.cfg.ExecutionMode,
		"kafka_enabled":  h.cfg.KafkaEnabled,
	})
}

func (h *DashboardHandler) Summary(w http.ResponseWriter, r *http.Request) {
	summary, err := h.dashboard.Summary(r.Context())
	if err != nil {
		response.Error(w, http.StatusInternalServerError, err)
		return
	}
	response.JSON(w, http.StatusOK, summary)
}

func (h *DashboardHandler) Markets(w http.ResponseWriter, r *http.Request) {
	items, err := h.store.ListMarkets(r.Context())
	writeList(w, items, err)
}

func (h *DashboardHandler) News(w http.ResponseWriter, r *http.Request) {
	items, err := h.store.ListNewsArticles(r.Context(), 100)
	writeList(w, items, err)
}

func (h *DashboardHandler) Signals(w http.ResponseWriter, r *http.Request) {
	items, err := h.store.ListAISignals(r.Context(), "", "")
	writeList(w, items, err)
}

func (h *DashboardHandler) ProbabilityDecisions(w http.ResponseWriter, r *http.Request) {
	items, err := h.store.ListProbabilityDecisions(r.Context(), 100)
	writeList(w, items, err)
}

func (h *DashboardHandler) RiskDecisions(w http.ResponseWriter, r *http.Request) {
	items, err := h.store.ListRiskDecisions(r.Context(), 100)
	writeList(w, items, err)
}

func (h *DashboardHandler) Trades(w http.ResponseWriter, r *http.Request) {
	items, err := h.store.ListTrades(r.Context(), 100)
	writeList(w, items, err)
}

func (h *DashboardHandler) Positions(w http.ResponseWriter, r *http.Request) {
	items, err := h.store.ListPositions(r.Context(), "")
	writeList(w, items, err)
}

func (h *DashboardHandler) Portfolio(w http.ResponseWriter, r *http.Request) {
	item, err := h.store.Portfolio(r.Context(), h.cfg.PaperStartingBalanceUSD)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, err)
		return
	}
	response.JSON(w, http.StatusOK, item)
}

func (h *DashboardHandler) Audit(w http.ResponseWriter, r *http.Request) {
	items, err := h.store.ListAudit(r.Context(), 100)
	writeList(w, items, err)
}

func writeList(w http.ResponseWriter, items any, err error) {
	if err != nil {
		response.Error(w, http.StatusInternalServerError, err)
		return
	}
	response.JSON(w, http.StatusOK, items)
}
