package service

import (
	"context"
	"strings"
	"sync"
	"time"

	dashboardmodel "github.com/local/polymarket-process-service/app/dashboard/model"
	"github.com/local/polymarket-process-service/pkg/eventbus"
	"github.com/local/polymarket-process-service/pkg/kafka"
)

const subscriberBuffer = 32

type StreamService struct {
	mu          sync.RWMutex
	nextID      int
	subscribers map[int]chan dashboardmodel.DashboardEvent
	lastSent    map[string]time.Time
	throttle    time.Duration
}

func NewStreamService(marketThrottle time.Duration) *StreamService {
	if marketThrottle <= 0 {
		marketThrottle = time.Second
	}
	return &StreamService{
		subscribers: map[int]chan dashboardmodel.DashboardEvent{},
		lastSent:    map[string]time.Time{},
		throttle:    marketThrottle,
	}
}

func (s *StreamService) Subscribe() (int, <-chan dashboardmodel.DashboardEvent, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	id := s.nextID
	ch := make(chan dashboardmodel.DashboardEvent, subscriberBuffer)
	s.subscribers[id] = ch
	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if existing, ok := s.subscribers[id]; ok {
				delete(s.subscribers, id)
				close(existing)
			}
		})
	}
	return id, ch, unsubscribe
}

func (s *StreamService) SubscriberCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subscribers)
}

func (s *StreamService) Publish(ctx context.Context, event dashboardmodel.DashboardEvent) {
	if s == nil || event.Type == "" || ctx.Err() != nil {
		return
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	if s.throttled(event) {
		return
	}
	s.mu.RLock()
	subscribers := make([]chan dashboardmodel.DashboardEvent, 0, len(s.subscribers))
	for _, ch := range s.subscribers {
		subscribers = append(subscribers, ch)
	}
	s.mu.RUnlock()
	for _, ch := range subscribers {
		offer(ch, event)
	}
}

func (s *StreamService) PublishEventBus(ctx context.Context, event eventbus.Event) {
	s.Publish(ctx, FromTopic(event.Topic, event.Data))
}

func (s *StreamService) PublishEnvelope(ctx context.Context, envelope kafka.EventEnvelope) {
	s.Publish(ctx, FromEnvelope(envelope))
}

func (s *StreamService) throttled(event dashboardmodel.DashboardEvent) bool {
	key := throttleKey(event)
	if key == "" {
		return false
	}
	now := event.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if last, ok := s.lastSent[key]; ok && now.Sub(last) < s.throttle {
		return true
	}
	s.lastSent[key] = now
	return false
}

func offer(ch chan dashboardmodel.DashboardEvent, event dashboardmodel.DashboardEvent) {
	select {
	case ch <- event:
		return
	default:
	}
	if isNonCritical(event.Type) {
		select {
		case old := <-ch:
			if !isNonCritical(old.Type) {
				select {
				case ch <- old:
				default:
				}
				return
			}
		default:
		}
		select {
		case ch <- event:
		default:
		}
		return
	}
	select {
	case ch <- event:
	default:
	}
}

func throttleKey(event dashboardmodel.DashboardEvent) string {
	switch event.Type {
	case dashboardmodel.EventMarketStateUpdated:
		return "market:" + event.EntityID
	case dashboardmodel.EventPositionUpdated:
		return "position:" + event.EntityID
	case dashboardmodel.EventPortfolioUpdated:
		return "portfolio"
	default:
		return ""
	}
}

func isNonCritical(eventType string) bool {
	switch eventType {
	case dashboardmodel.EventMarketStateUpdated, dashboardmodel.EventPositionUpdated, dashboardmodel.EventPortfolioUpdated:
		return true
	default:
		return false
	}
}

func FromEnvelope(envelope kafka.EventEnvelope) dashboardmodel.DashboardEvent {
	event := FromTopic(envelope.EventType, nil)
	event.EntityID = envelope.Key
	event.Payload = map[string]any{
		"event_id":       envelope.EventID,
		"event_type":     envelope.EventType,
		"source":         envelope.Source,
		"correlation_id": envelope.CorrelationID,
		"payload":        envelope.Payload,
	}
	event.CreatedAt = envelope.OccurredAt
	return event
}

func FromTopic(topic string, payload any) dashboardmodel.DashboardEvent {
	return dashboardmodel.DashboardEvent{
		Type:      eventTypeForTopic(topic),
		EntityID:  kafka.KeyFromPayload(topic, payload),
		Payload:   map[string]any{"topic": topic, "data": payload},
		CreatedAt: time.Now().UTC(),
	}
}

func eventTypeForTopic(topic string) string {
	switch topic {
	case kafka.TopicMarketSelected, kafka.TopicMarketPriceUpdated, kafka.TopicMarketOrderBookUpdated, kafka.TopicMarketTradeExecuted:
		return dashboardmodel.EventMarketStateUpdated
	case kafka.TopicNewsArrived:
		return dashboardmodel.EventNewsArrived
	case kafka.TopicNewsMarketMatched:
		return dashboardmodel.EventNewsMatched
	case kafka.TopicAISignalGenerated:
		return dashboardmodel.EventAISignalGenerated
	case kafka.TopicProbabilityCandidate, kafka.TopicProbabilityNoTrade:
		return dashboardmodel.EventProbabilityDecisionCreated
	case kafka.TopicRiskApproved:
		return dashboardmodel.EventRiskApproved
	case kafka.TopicRiskRejected:
		return dashboardmodel.EventRiskRejected
	case kafka.TopicTradeOpened:
		return dashboardmodel.EventPaperTradeOpened
	case kafka.TopicTradeClosed:
		return dashboardmodel.EventPaperTradeClosed
	case kafka.TopicPositionUpdated:
		return dashboardmodel.EventPositionUpdated
	case kafka.TopicPortfolioUpdated:
		return dashboardmodel.EventPortfolioUpdated
	case kafka.TopicAuditCreated:
		return dashboardmodel.EventAuditCreated
	default:
		if strings.HasPrefix(topic, "dlq.") {
			return dashboardmodel.EventError
		}
		return strings.ReplaceAll(topic, ".", "_")
	}
}

type EventBusStream struct {
	stream *StreamService
}

func NewEventBusStream(stream *StreamService) *EventBusStream {
	return &EventBusStream{stream: stream}
}

func (b *EventBusStream) Publish(ctx context.Context, event eventbus.Event) {
	if b == nil || b.stream == nil {
		return
	}
	b.stream.PublishEventBus(ctx, event)
}

func (b *EventBusStream) Subscribe(string, eventbus.Handler) {}
