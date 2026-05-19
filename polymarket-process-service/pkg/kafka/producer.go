package kafka

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/IBM/sarama"
)

type Producer struct {
	client sarama.SyncProducer
	source string
	log    *slog.Logger
}

func NewProducer(cfg Config, source string, log *slog.Logger) (*Producer, error) {
	client, err := sarama.NewSyncProducer(cfg.Brokers, saramaConfig(cfg.ClientID, cfg.AutoCreateTopics))
	if err != nil {
		return nil, err
	}
	return NewProducerWithClient(client, source, log), nil
}

func NewProducerWithClient(client sarama.SyncProducer, source string, log *slog.Logger) *Producer {
	if log == nil {
		log = slog.Default()
	}
	return &Producer{client: client, source: source, log: log}
}

func (p *Producer) Publish(ctx context.Context, topic, key string, payload any, options ...PublishOption) error {
	if p == nil || p.client == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	envelope, err := NewEnvelope(topic, key, p.source, payload, options...)
	if err != nil {
		return err
	}
	return p.PublishEnvelope(ctx, topic, envelope.Key, envelope)
}

func (p *Producer) PublishEnvelope(ctx context.Context, topic, key string, envelope EventEnvelope) error {
	if p == nil || p.client == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if envelope.EventType == "" {
		envelope.EventType = topic
	}
	if envelope.Source == "" {
		envelope.Source = p.source
	}
	if envelope.Key == "" {
		envelope.Key = key
	}
	if key == "" {
		key = envelope.Key
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(raw),
		Headers: []sarama.RecordHeader{
			{Key: []byte("event_id"), Value: []byte(envelope.EventID)},
			{Key: []byte("event_type"), Value: []byte(envelope.EventType)},
			{Key: []byte("source"), Value: []byte(envelope.Source)},
			{Key: []byte("correlation_id"), Value: []byte(envelope.CorrelationID)},
		},
	}
	_, _, err = p.client.SendMessage(message)
	if err != nil {
		p.log.Error("kafka_publish_failed", "topic", topic, "key", key, "event_id", envelope.EventID, "correlation_id", envelope.CorrelationID, "error", err)
		return err
	}
	p.log.Debug("kafka_published", "topic", topic, "key", key, "event_id", envelope.EventID, "correlation_id", envelope.CorrelationID)
	return nil
}

func (p *Producer) PublishDLQ(ctx context.Context, originalTopic, key string, original EventEnvelope, cause error) error {
	payload := map[string]any{
		"original_topic": originalTopic,
		"failed_event":   original,
	}
	if cause != nil {
		payload["error"] = cause.Error()
	}
	return p.Publish(ctx, DLQTopic(originalTopic), key, payload,
		WithEventType("dlq."+original.EventType),
		WithCorrelationID(original.CorrelationID),
	)
}

func (p *Producer) Close() error {
	if p == nil || p.client == nil {
		return nil
	}
	return p.client.Close()
}
