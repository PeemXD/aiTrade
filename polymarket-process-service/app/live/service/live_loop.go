package service

import (
	"context"
	"time"
)

func (s *Service) Start(ctx context.Context) bool {
	s.loopMu.Lock()
	defer s.loopMu.Unlock()
	if s.running {
		return false
	}
	loopCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.running = true
	go s.loop(loopCtx)
	return true
}

func (s *Service) Stop() bool {
	s.loopMu.Lock()
	defer s.loopMu.Unlock()
	if !s.running {
		return false
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.running = false
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
	interval := s.cfg.LiveLoopInterval
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		_, _ = s.RunOnce(ctx)
		select {
		case <-ctx.Done():
			s.loopMu.Lock()
			s.running = false
			s.loopMu.Unlock()
			return
		case <-ticker.C:
		}
	}
}
