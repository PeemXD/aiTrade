package handler

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	dashboardmodel "github.com/local/polymarket-process-service/app/dashboard/model"
	dashboardservice "github.com/local/polymarket-process-service/app/dashboard/service"
	"github.com/stretchr/testify/require"
)

func TestSSEConnectsSendsConnectedHeartbeatAndPublishedEvent(t *testing.T) {
	stream := dashboardservice.NewStreamService(time.Millisecond)
	handler := NewSSEHandler(stream, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/stream", nil).WithContext(ctx)
	rec := newStreamRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.Stream(rec, req)
	}()
	require.Contains(t, rec.waitFor(t, "event: "+dashboardmodel.EventConnected), "event: connected")
	require.Equal(t, "text/event-stream", strings.Split(rec.Header().Get("Content-Type"), ";")[0])
	stream.Publish(context.Background(), dashboardmodel.DashboardEvent{Type: dashboardmodel.EventAuditCreated, EntityID: "audit-1"})
	require.Contains(t, rec.waitFor(t, "event: "+dashboardmodel.EventAuditCreated), "event: audit_created")
	require.Contains(t, rec.waitFor(t, "event: "+dashboardmodel.EventHeartbeat), "event: heartbeat")
	cancel()
	<-done
	require.Eventually(t, func() bool { return stream.SubscriberCount() == 0 }, time.Second, 10*time.Millisecond)
}

type streamRecorder struct {
	mu     sync.Mutex
	cond   *sync.Cond
	header http.Header
	body   bytes.Buffer
	status int
}

func newStreamRecorder() *streamRecorder {
	rec := &streamRecorder{header: http.Header{}, status: http.StatusOK}
	rec.cond = sync.NewCond(&rec.mu)
	return rec
}

func (r *streamRecorder) Header() http.Header {
	return r.header
}

func (r *streamRecorder) WriteHeader(status int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status = status
	r.cond.Broadcast()
}

func (r *streamRecorder) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, err := r.body.Write(p)
	r.cond.Broadcast()
	return n, err
}

func (r *streamRecorder) Flush() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cond.Broadcast()
}

func (r *streamRecorder) waitFor(t *testing.T, marker string) string {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		out := r.body.String()
		if strings.Contains(out, marker) {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s in %q", marker, out)
		}
		r.cond.Wait()
	}
}
