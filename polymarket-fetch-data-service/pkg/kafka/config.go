package kafka

import (
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type Config struct {
	Enabled          bool
	Brokers          []string
	ClientID         string
	ConsumerGroup    string
	AutoCreateTopics bool
}

func NewConfig(enabled bool, brokers []string, clientID, consumerGroup string, autoCreateTopics bool) Config {
	return Config{
		Enabled:          enabled,
		Brokers:          brokers,
		ClientID:         strings.TrimSpace(clientID),
		ConsumerGroup:    strings.TrimSpace(consumerGroup),
		AutoCreateTopics: autoCreateTopics,
	}
}

func saramaConfig(clientID string, autoCreateTopics bool) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_7_0_0
	cfg.ClientID = clientID
	cfg.Metadata.AllowAutoTopicCreation = autoCreateTopics
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Timeout = 10 * time.Second
	cfg.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	return cfg
}
