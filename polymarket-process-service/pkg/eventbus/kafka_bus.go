package eventbus

import (
	"context"
	"log/slog"

	"github.com/local/polymarket-process-service/pkg/kafka"
)

type KafkaBus struct {
	producer *kafka.Producer
	log      *slog.Logger
}

func NewKafkaBus(producer *kafka.Producer, log *slog.Logger) *KafkaBus {
	if log == nil {
		log = slog.Default()
	}
	return &KafkaBus{producer: producer, log: log}
}

func (b *KafkaBus) Publish(ctx context.Context, event Event) {
	if b == nil || b.producer == nil || event.Topic == "" {
		return
	}
	if !PipelineEventsEnabled(ctx) {
		return
	}
	if err := b.producer.Publish(ctx, event.Topic, kafka.KeyFromPayload(event.Topic, event.Data), event.Data); err != nil {
		b.log.Error("eventbus_kafka_publish_failed", "topic", event.Topic, "error", err)
	}
}

func (b *KafkaBus) Subscribe(string, Handler) {}

type Composite struct {
	buses []Bus
}

func NewComposite(buses ...Bus) *Composite {
	out := &Composite{}
	for _, bus := range buses {
		if bus != nil {
			out.buses = append(out.buses, bus)
		}
	}
	return out
}

func (b *Composite) Publish(ctx context.Context, event Event) {
	if b == nil {
		return
	}
	for _, bus := range b.buses {
		bus.Publish(ctx, event)
	}
}

func (b *Composite) Subscribe(topic string, handler Handler) {
	if b == nil {
		return
	}
	for _, bus := range b.buses {
		bus.Subscribe(topic, handler)
	}
}
