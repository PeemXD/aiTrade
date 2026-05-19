package kafka

import (
	"context"
	"time"
)

type RetryConfig struct {
	MaxAttempts int
	Backoff     time.Duration
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{MaxAttempts: 3, Backoff: 250 * time.Millisecond}
}

func retry(ctx context.Context, cfg RetryConfig, fn func() error) error {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}
	var err error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if attempt == cfg.MaxAttempts || cfg.Backoff <= 0 {
			continue
		}
		timer := time.NewTimer(cfg.Backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}
