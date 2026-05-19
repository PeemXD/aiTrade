package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/IBM/sarama"
	saramamocks "github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/require"
)

func TestProducerMarshalsEnvelopeAndSetsKey(t *testing.T) {
	mock := saramamocks.NewSyncProducer(t, nil)
	mock.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		require.Equal(t, TopicMarketSelected, msg.Topic)
		key, err := msg.Key.Encode()
		require.NoError(t, err)
		require.Equal(t, "market-1", string(key))
		value, err := msg.Value.Encode()
		require.NoError(t, err)
		var envelope EventEnvelope
		require.NoError(t, json.Unmarshal(value, &envelope))
		require.NotEmpty(t, envelope.EventID)
		require.Equal(t, TopicMarketSelected, envelope.EventType)
		require.Equal(t, "process-test", envelope.Source)
		require.Equal(t, "market-1", envelope.Key)
		var payload map[string]any
		require.NoError(t, json.Unmarshal(envelope.Payload, &payload))
		require.Equal(t, "market-1", payload["market_id"])
		return nil
	})
	producer := NewProducerWithClient(mock, "process-test", discardLogger())
	err := producer.Publish(context.Background(), TopicMarketSelected, "", map[string]any{"market_id": "market-1"})
	require.NoError(t, err)
	require.NoError(t, producer.Close())
}

func TestConsumerCommitsOnlyAfterSuccess(t *testing.T) {
	raw := mustEnvelopeJSON(t, TopicNewsArrived, "news-1")
	committed := false
	c := &Consumer{
		handlers: map[string]HandlerFunc{
			TopicNewsArrived: func(context.Context, EventEnvelope) error {
				require.False(t, committed)
				return nil
			},
		},
		retry: DefaultRetryConfig(),
		log:   discardLogger(),
	}
	err := c.HandleMessage(context.Background(), Message{Topic: TopicNewsArrived, Key: "news-1", Value: raw}, func() {
		committed = true
	})
	require.NoError(t, err)
	require.True(t, committed)
}

func TestConsumerRetriesFailedMessage(t *testing.T) {
	raw := mustEnvelopeJSON(t, TopicAISignalGenerated, "market-1")
	attempts := 0
	committed := false
	c := &Consumer{
		handlers: map[string]HandlerFunc{
			TopicAISignalGenerated: func(context.Context, EventEnvelope) error {
				attempts++
				if attempts < 3 {
					return errors.New("transient")
				}
				return nil
			},
		},
		retry: RetryConfig{MaxAttempts: 3},
		log:   discardLogger(),
	}
	err := c.HandleMessage(context.Background(), Message{Topic: TopicAISignalGenerated, Key: "market-1", Value: raw}, func() {
		committed = true
	})
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
	require.True(t, committed)
}

func TestConsumerSendsPoisonMessageToDLQ(t *testing.T) {
	mock := saramamocks.NewSyncProducer(t, nil)
	mock.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		require.Equal(t, TopicDLQProcess, msg.Topic)
		value, err := msg.Value.Encode()
		require.NoError(t, err)
		var envelope EventEnvelope
		require.NoError(t, json.Unmarshal(value, &envelope))
		require.Equal(t, "dlq."+TopicAISignalGenerated, envelope.EventType)
		return nil
	})
	producer := NewProducerWithClient(mock, "process-test", discardLogger())
	raw := mustEnvelopeJSON(t, TopicAISignalGenerated, "market-1")
	attempts := 0
	committed := false
	c := &Consumer{
		handlers: map[string]HandlerFunc{
			TopicAISignalGenerated: func(context.Context, EventEnvelope) error {
				attempts++
				return errors.New("poison")
			},
		},
		producer: producer,
		retry:    RetryConfig{MaxAttempts: 2},
		log:      discardLogger(),
	}
	err := c.HandleMessage(context.Background(), Message{Topic: TopicAISignalGenerated, Key: "market-1", Value: raw}, func() {
		committed = true
	})
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	require.True(t, committed)
	require.NoError(t, producer.Close())
}

func mustEnvelopeJSON(t *testing.T, topic, key string) []byte {
	t.Helper()
	envelope, err := NewEnvelope(topic, key, "test", map[string]any{"id": key})
	require.NoError(t, err)
	raw, err := json.Marshal(envelope)
	require.NoError(t, err)
	return raw
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
