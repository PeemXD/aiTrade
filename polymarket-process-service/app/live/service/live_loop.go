package service

import (
	"context"
	"time"
)

func (s *Service) Start(ctx context.Context) bool {
	s.loopMu.Lock()
	defer s.loopMu.Unlock()
	if s.running {
		s.log.Info("live_loop_start_ignored", "reason", "already_running")
		return false
	}
	loopCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.running = true
	s.stopping = false
	s.log.Info("live_loop_started", "interval_seconds", int(s.loopInterval().Seconds()))
	go s.loop(loopCtx)
	return true
}

func (s *Service) Stop() bool {
	s.loopMu.Lock()
	defer s.loopMu.Unlock()
	if !s.running {
		s.log.Info("live_loop_stop_ignored", "reason", "not_running")
		return false
	}
	if s.stopping {
		s.log.Info("live_loop_stop_ignored", "reason", "already_stopping")
		return false
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.stopping = true
	s.log.Info("live_loop_stop_requested")
	return true
}

func (s *Service) Status() string {
	s.loopMu.Lock()
	defer s.loopMu.Unlock()
	if s.running {
		return "running"
	}
	return "stopped"
}

func (s *Service) loop(ctx context.Context) {
	interval := s.loopInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer func() {
		if recovered := recover(); recovered != nil {
			s.log.Error("live_loop_panic_recovered", "panic", recovered)
		}
		s.loopMu.Lock()
		s.running = false
		s.stopping = false
		s.cancel = nil
		s.loopMu.Unlock()
		s.log.Info("live_loop_stopped")
	}()
	for {
		result, err := s.RunOnce(ctx)
		if err != nil {
			s.recordError(ctx, &result, "live_loop_run_once_failed", "live loop RunOnce failed", err)
		}
		s.log.Info("live_loop_cycle_completed",
			"markets_loaded", result.MarketsLoaded,
			"articles_loaded", result.ArticlesLoaded,
			"matches_created", result.MatchesCreated,
			"paper_trades_opened", result.PaperTradesOpened,
			"errors", len(result.Events),
		)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) loopInterval() time.Duration {
	interval := s.cfg.LiveLoopInterval
	if interval <= 0 {
		interval = time.Minute
	}
	return interval
}
