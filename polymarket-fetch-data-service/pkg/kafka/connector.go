package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"
)

func WaitForKafka(ctx context.Context, brokers []string, log *slog.Logger) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no kafka brokers configured")
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastErr error
	for {
		for _, broker := range brokers {
			if err := dialBroker(ctx, broker); err != nil {
				lastErr = err
				continue
			}
			log.Info("kafka_connected", "broker", broker)
			return nil
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return lastErr
			}
			return ctx.Err()
		case <-ticker.C:
			log.Info("waiting_for_kafka", "brokers", strings.Join(brokers, ","))
		}
	}
}

func dialBroker(ctx context.Context, broker string) error {
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	return conn.Close()
}
