package eventbus

import (
	"context"
	"sync"
)

type Event struct {
	Topic string
	Data  any
}

type Handler func(context.Context, Event)

type Bus interface {
	Publish(context.Context, Event)
	Subscribe(topic string, handler Handler)
}

type InMemory struct {
	mu       sync.RWMutex
	handlers map[string][]Handler
}

func NewInMemory() *InMemory {
	return &InMemory{handlers: map[string][]Handler{}}
}

func (b *InMemory) Publish(ctx context.Context, event Event) {
	b.mu.RLock()
	handlers := append([]Handler(nil), b.handlers[event.Topic]...)
	b.mu.RUnlock()
	for _, h := range handlers {
		go h(ctx, event)
	}
}

func (b *InMemory) Subscribe(topic string, handler Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[topic] = append(b.handlers[topic], handler)
}
