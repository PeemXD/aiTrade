package handler

import (
	"net/http"
	"strings"

	liveservice "github.com/local/polymarket-process-service/app/live/service"
	"github.com/local/polymarket-process-service/pkg/response"
)

type LiveHandler struct {
	service *liveservice.Service
}

func NewLiveHandler(service *liveservice.Service) *LiveHandler {
	return &LiveHandler{service: service}
}

func (h *LiveHandler) RunOnce(w http.ResponseWriter, r *http.Request) {
	result, err := h.service.RunOnce(r.Context())
	if err != nil {
		response.Error(w, http.StatusInternalServerError, err)
		return
	}
	response.JSON(w, http.StatusOK, result)
}

func (h *LiveHandler) Start(w http.ResponseWriter, r *http.Request) {
	started := h.service.Start(r.Context())
	response.JSON(w, http.StatusOK, map[string]any{"status": h.service.Status(), "started": started})
}

func (h *LiveHandler) Stop(w http.ResponseWriter, _ *http.Request) {
	stopped := h.service.Stop()
	response.JSON(w, http.StatusOK, map[string]any{"status": h.service.Status(), "stopped": stopped})
}

func (h *LiveHandler) ClosePosition(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/positions/"), "/close")
	id = strings.Trim(id, "/")
	if id == "" {
		response.Error(w, http.StatusBadRequest, errMissingPositionID{})
		return
	}
	if err := h.service.ClosePosition(r.Context(), id); err != nil {
		response.Error(w, http.StatusInternalServerError, err)
		return
	}
	response.JSON(w, http.StatusOK, map[string]any{"position_id": id, "status": "close_requested"})
}

type errMissingPositionID struct{}

func (errMissingPositionID) Error() string { return "position id is required" }
