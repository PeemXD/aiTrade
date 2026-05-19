package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	dashboardmodel "github.com/local/polymarket-process-service/app/dashboard/model"
	dashboardservice "github.com/local/polymarket-process-service/app/dashboard/service"
)

type SSEHandler struct {
	stream    *dashboardservice.StreamService
	heartbeat time.Duration
}

func NewSSEHandler(stream *dashboardservice.StreamService, heartbeat time.Duration) *SSEHandler {
	if heartbeat <= 0 {
		heartbeat = 15 * time.Second
	}
	return &SSEHandler{stream: stream, heartbeat: heartbeat}
}

func (h *SSEHandler) Stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	_, events, unsubscribe := h.stream.Subscribe()
	defer unsubscribe()
	writeSSE(w, dashboardmodel.DashboardEvent{Type: dashboardmodel.EventConnected, CreatedAt: time.Now().UTC()})
	flusher.Flush()
	ticker := time.NewTicker(h.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			writeSSE(w, event)
			flusher.Flush()
		case <-ticker.C:
			writeSSE(w, dashboardmodel.DashboardEvent{Type: dashboardmodel.EventHeartbeat, CreatedAt: time.Now().UTC()})
			flusher.Flush()
		}
	}
}

func writeSSE(w http.ResponseWriter, event dashboardmodel.DashboardEvent) {
	raw, err := json.Marshal(event)
	if err != nil {
		event = dashboardmodel.DashboardEvent{Type: dashboardmodel.EventError, Payload: map[string]any{"error": err.Error()}, CreatedAt: time.Now().UTC()}
		raw, _ = json.Marshal(event)
	}
	_, _ = fmt.Fprintf(w, "event: %s\n", event.Type)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", raw)
}
