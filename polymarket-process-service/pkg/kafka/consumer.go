package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
)

type Consumer struct {
	group    sarama.ConsumerGroup
	topics   []string
	handlers map[string]HandlerFunc
	producer *Producer
	retry    RetryConfig
	log      *slog.Logger
}

func NewConsumer(cfg Config, groupID string, topics []string, handlers map[string]HandlerFunc, producer *Producer, log *slog.Logger) (*Consumer, error) {
	if log == nil {
		log = slog.Default()
	}
	group, err := sarama.NewConsumerGroup(cfg.Brokers, groupID, saramaConfig(cfg.ClientID, cfg.AutoCreateTopics))
	if err != nil {
		return nil, err
	}
	return &Consumer{
		group:    group,
		topics:   append([]string(nil), topics...),
		handlers: handlers,
		producer: producer,
		retry:    DefaultRetryConfig(),
		log:      log,
	}, nil
}

func (c *Consumer) SetRetryConfig(cfg RetryConfig) {
	c.retry = cfg
}

func (c *Consumer) Start(ctx context.Context) error {
	if c == nil || c.group == nil {
		return nil
	}
	handler := consumerGroupHandler{consumer: c}
	for ctx.Err() == nil {
		if err := c.group.Consume(ctx, c.topics, handler); err != nil {
			c.log.Error("kafka_consume_failed", "topics", c.topics, "error", err)
		}
	}
	return ctx.Err()
}

func (c *Consumer) Close() error {
	if c == nil || c.group == nil {
		return nil
	}
	return c.group.Close()
}

func (c *Consumer) HandleMessage(ctx context.Context, msg Message, commit CommitFunc) error {
	var envelope EventEnvelope
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		c.log.Warn("kafka_bad_envelope", "topic", msg.Topic, "error", err)
		return c.publishRawDLQ(ctx, msg, err, commit)
	}
	handler := c.handlers[msg.Topic]
	if handler == nil {
		handler = c.handlers[envelope.EventType]
	}
	if handler == nil {
		c.log.Warn("kafka_no_handler", "topic", msg.Topic, "event_type", envelope.EventType, "event_id", envelope.EventID)
		if commit != nil {
			commit()
		}
		return nil
	}
	err := retry(ctx, c.retry, func() error {
		return handler(ctx, envelope)
	})
	if err != nil {
		c.log.Error("kafka_handler_failed", "topic", msg.Topic, "event_id", envelope.EventID, "correlation_id", envelope.CorrelationID, "error", err)
		if c.producer == nil {
			return err
		}
		if dlqErr := c.producer.PublishDLQ(ctx, msg.Topic, msg.Key, envelope, err); dlqErr != nil {
			return dlqErr
		}
		if commit != nil {
			commit()
		}
		return nil
	}
	if commit != nil {
		commit()
	}
	c.log.Debug("kafka_message_processed", "topic", msg.Topic, "event_id", envelope.EventID, "correlation_id", envelope.CorrelationID)
	return nil
}

func (c *Consumer) publishRawDLQ(ctx context.Context, msg Message, cause error, commit CommitFunc) error {
	if c.producer == nil {
		return cause
	}
	envelope := EventEnvelope{
		EventType: "invalid.envelope",
		Key:       msg.Key,
		Payload:   json.RawMessage(fmt.Sprintf("%q", string(msg.Value))),
	}
	if err := c.producer.PublishDLQ(ctx, msg.Topic, msg.Key, envelope, cause); err != nil {
		return err
	}
	if commit != nil {
		commit()
	}
	return nil
}

type consumerGroupHandler struct {
	consumer *Consumer
}

func (h consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		key := ""
		if msg.Key != nil {
			key = string(msg.Key)
		}
		_ = h.consumer.HandleMessage(session.Context(), Message{
			Topic: msg.Topic,
			Key:   key,
			Value: msg.Value,
		}, func() {
			session.MarkMessage(msg, "")
			session.Commit()
		})
	}
	return nil
}
