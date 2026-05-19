package service

import (
	"context"
	"testing"
	"time"

	dashboardmodel "github.com/local/polymarket-process-service/app/dashboard/model"
	"github.com/stretchr/testify/require"
)

func TestStreamServiceStreamsPublishedEventAndCleansSubscriber(t *testing.T) {
	stream := NewStreamService(time.Second)
	_, events, unsubscribe := stream.Subscribe()
	require.Equal(t, 1, stream.SubscriberCount())
	stream.Publish(context.Background(), dashboardmodel.DashboardEvent{Type: dashboardmodel.EventAuditCreated, EntityID: "audit-1"})
	select {
	case event := <-events:
		require.Equal(t, dashboardmodel.EventAuditCreated, event.Type)
		require.Equal(t, "audit-1", event.EntityID)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stream event")
	}
	unsubscribe()
	require.Equal(t, 0, stream.SubscriberCount())
}

func TestSlowSubscriberDoesNotBlockStreamService(t *testing.T) {
	stream := NewStreamService(0)
	_, _, unsubscribe := stream.Subscribe()
	defer unsubscribe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < subscriberBuffer*4; i++ {
			stream.Publish(context.Background(), dashboardmodel.DashboardEvent{
				Type:     dashboardmodel.EventMarketStateUpdated,
				EntityID: "market-1",
			})
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("slow subscriber blocked stream publisher")
	}
}
