package runtime

import (
	"context"
	"log/slog"

	"github.com/local/polymarket-process-service/pkg/cache"
	"github.com/local/polymarket-process-service/pkg/config"
	"github.com/local/polymarket-process-service/pkg/kafka"
	"github.com/local/polymarket-process-service/pkg/repository"
)

func OpenStore(ctx context.Context, cfg config.Config, log *slog.Logger) repository.Store {
	store, err := repository.NewPostgresStore(ctx, cfg.DatabaseURL)
	if err == nil {
		return store
	}
	log.Warn("postgres unavailable, using in-memory store", "error", err)
	return repository.NewMemoryStore()
}

func OpenCache(ctx context.Context, cfg config.Config, log *slog.Logger) cache.Cache {
	redisCache := cache.NewRedis(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err := redisCache.Ping(ctx); err == nil {
		return redisCache
	}
	log.Warn("redis unavailable, using in-memory cache")
	return cache.NewMemory()
}

func ConnectKafka(ctx context.Context, cfg config.Config, log *slog.Logger, serviceName string) error {
	if !cfg.KafkaEnabled || cfg.EventBus != "kafka" {
		log.Info("kafka_disabled", "service", serviceName)
		return nil
	}
	log.Info("connecting_to_kafka", "service", serviceName, "brokers", cfg.KafkaBrokers)
	return kafka.WaitForKafka(ctx, cfg.KafkaBrokers, log)
}

func OpenKafkaProducer(cfg config.Config, log *slog.Logger, serviceName string) (*kafka.Producer, error) {
	if !cfg.KafkaEnabled || cfg.EventBus != "kafka" {
		return nil, nil
	}
	return kafka.NewProducer(kafka.NewConfig(cfg.KafkaEnabled, cfg.KafkaBrokers, cfg.KafkaClientID, cfg.KafkaConsumerGroupProcess, cfg.KafkaAutoCreateTopics), serviceName, log)
}
