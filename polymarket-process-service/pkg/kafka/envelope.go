package kafka

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

type EventEnvelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	Source        string          `json:"source"`
	Version       int             `json:"version"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	Key           string          `json:"key,omitempty"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
}

type PublishOptions struct {
	EventID       string
	EventType     string
	Source        string
	CorrelationID string
	OccurredAt    time.Time
}

type PublishOption func(*PublishOptions)

func WithEventID(eventID string) PublishOption {
	return func(opts *PublishOptions) { opts.EventID = eventID }
}

func WithEventType(eventType string) PublishOption {
	return func(opts *PublishOptions) { opts.EventType = eventType }
}

func WithSource(source string) PublishOption {
	return func(opts *PublishOptions) { opts.Source = source }
}

func WithCorrelationID(correlationID string) PublishOption {
	return func(opts *PublishOptions) { opts.CorrelationID = correlationID }
}

func WithOccurredAt(occurredAt time.Time) PublishOption {
	return func(opts *PublishOptions) { opts.OccurredAt = occurredAt }
}

func NewEnvelope(topic, key, source string, payload any, options ...PublishOption) (EventEnvelope, error) {
	opts := PublishOptions{
		EventID:    uuid.NewString(),
		EventType:  topic,
		Source:     source,
		OccurredAt: time.Now().UTC(),
	}
	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}
	if opts.EventID == "" {
		opts.EventID = uuid.NewString()
	}
	if opts.EventType == "" {
		opts.EventType = topic
	}
	if opts.Source == "" {
		opts.Source = source
	}
	if opts.OccurredAt.IsZero() {
		opts.OccurredAt = time.Now().UTC()
	}
	if opts.CorrelationID == "" {
		opts.CorrelationID = opts.EventID
	}
	payloadBytes, err := marshalPayload(payload)
	if err != nil {
		return EventEnvelope{}, err
	}
	if key == "" {
		key = KeyFromPayload(topic, payload)
	}
	return EventEnvelope{
		EventID:       opts.EventID,
		EventType:     opts.EventType,
		Source:        opts.Source,
		Version:       1,
		CorrelationID: opts.CorrelationID,
		Key:           key,
		OccurredAt:    opts.OccurredAt,
		Payload:       payloadBytes,
	}, nil
}

func marshalPayload(payload any) (json.RawMessage, error) {
	switch v := payload.(type) {
	case nil:
		return json.RawMessage(`{}`), nil
	case json.RawMessage:
		return v, nil
	case []byte:
		if json.Valid(v) {
			return json.RawMessage(v), nil
		}
	}
	raw, err := json.Marshal(payload)
	return json.RawMessage(raw), err
}

func KeyFromPayload(topic string, payload any) string {
	var data map[string]any
	raw, err := marshalPayload(payload)
	if err != nil {
		return ""
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return ""
	}
	switch {
	case strings.HasPrefix(topic, "market."):
		return firstString(data, "market_id", "id")
	case strings.HasPrefix(topic, "news."):
		return firstString(data, "news_id", "id")
	case strings.HasPrefix(topic, "trade."):
		return firstString(data, "trade_id", "id")
	case strings.HasPrefix(topic, "position."):
		return firstString(data, "position_id", "id")
	case strings.HasPrefix(topic, "portfolio."):
		return "portfolio"
	default:
		return firstString(data, "market_id", "news_id", "trade_id", "position_id", "id")
	}
}

func firstString(data map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := data[key]; ok {
			if out, ok := value.(string); ok {
				return out
			}
		}
	}
	return ""
}
