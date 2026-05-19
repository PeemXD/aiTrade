package eventstream

import (
	"context"
	"sync"
	"time"
)

const defaultSubscriberBuffer = 32

type Event struct {
	Type      string         `json:"type"`
	EntityID  string         `json:"entity_id,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

type Broker struct {
	mu          sync.RWMutex
	buffer      int
	subscribers map[chan Event]struct{}
}

func NewBroker() *Broker {
	return &Broker{
		buffer:      defaultSubscriberBuffer,
		subscribers: map[chan Event]struct{}{},
	}
}

func (b *Broker) Subscribe() (<-chan Event, func()) {
	if b == nil {
		ch := make(chan Event)
		close(ch)
		return ch, func() {}
	}
	ch := make(chan Event, b.buffer)
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()
	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subscribers, ch)
			close(ch)
			b.mu.Unlock()
		})
	}
	return ch, unsubscribe
}

func (b *Broker) Publish(_ context.Context, event Event) {
	if b == nil || event.Type == "" {
		return
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}
