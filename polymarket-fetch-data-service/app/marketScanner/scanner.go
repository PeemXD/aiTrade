package marketscanner

import (
	"context"
	"github.com/local/polymarket-fetch-data-service/pkg/config"
	"github.com/local/polymarket-fetch-data-service/pkg/eventbus"
	"github.com/local/polymarket-fetch-data-service/pkg/idgen"
	"github.com/local/polymarket-fetch-data-service/pkg/repository"
	"log/slog"
	"sort"
	"time"
)

type ScannerService struct {
	cfg    config.Config
	rest   RESTClient
	store  repository.Store
	logger *slog.Logger
	bus    eventbus.Bus
}

func NewScannerService(cfg config.Config, rest RESTClient, store repository.Store, logger *slog.Logger) *ScannerService {
	return &ScannerService{cfg: cfg, rest: rest, store: store, logger: logger}
}

func (s *ScannerService) SetEventBus(bus eventbus.Bus) {
	s.bus = bus
}

func (s *ScannerService) Refresh(ctx context.Context) (RefreshResult, error) {
	raw, err := s.rest.FetchMarkets(ctx, s.cfg.MaxSubscribedMarkets*4)
	if err != nil {
		return RefreshResult{Errors: []string{err.Error()}}, err
	}
	selected := make([]Market, 0, s.cfg.MaxSubscribedMarkets)
	errors := []string{}
	now := time.Now().UTC()
	for _, m := range raw {
		if !s.baseTradable(m, now) {
			continue
		}
		if m.YesTokenID != "" {
			book, err := s.rest.GetOrderBook(ctx, m.YesTokenID)
			if err != nil {
				errors = append(errors, err.Error())
			} else {
				m.OrderBook = book
				if len(book.Bids) > 0 {
					m.BestBid = book.Bids[0].Price
				}
				if len(book.Asks) > 0 {
					m.BestAsk = book.Asks[0].Price
				}
			}
			if mid, err := s.rest.GetMidpoint(ctx, m.YesTokenID); err == nil && mid > 0 {
				m.YesPrice = mid
			}
			if spread, err := s.rest.GetSpread(ctx, m.YesTokenID); err == nil && spread > 0 {
				m.Spread = spread
			}
		}
		if m.BestBid == 0 && m.YesPrice > 0 {
			m.BestBid = m.YesPrice
		}
		if m.BestAsk == 0 && m.YesPrice > 0 {
			m.BestAsk = m.YesPrice
		}
		if m.Spread == 0 && m.BestBid > 0 && m.BestAsk > 0 {
			m.Spread = m.BestAsk - m.BestBid
		}
		if !s.afterEnrichmentTradable(m) {
			continue
		}
		m.UpdatedAt = now
		if m.CreatedAt.IsZero() {
			m.CreatedAt = now
		}
		selected = append(selected, m)
		if len(selected) >= s.cfg.MaxSubscribedMarkets {
			break
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Volume > selected[j].Volume })
	if err := s.store.SaveMarkets(ctx, selected); err != nil {
		return RefreshResult{Selected: selected, Errors: append(errors, err.Error())}, err
	}
	for _, m := range selected {
		_ = s.store.SaveAudit(ctx, repository.AuditLog{
			ID: idgen.New(), Event: "market_selected", EntityID: m.ID,
			Payload:   map[string]any{"market_id": m.ID, "question": m.Question, "volume": m.Volume, "spread": m.Spread},
			CreatedAt: now,
		})
		if s.bus != nil {
			s.bus.Publish(ctx, eventbus.Event{Topic: "market.selected", Data: m})
			s.bus.Publish(ctx, eventbus.Event{Topic: "market.created", Data: m})
		}
		s.logger.Info("market_selected", "market_id", m.ID, "question", m.Question, "volume", m.Volume, "spread", m.Spread)
	}
	if s.bus != nil {
		s.bus.Publish(ctx, eventbus.Event{Topic: "market.refresh.completed", Data: RefreshResult{Selected: selected, Errors: errors}})
	}
	return RefreshResult{Selected: selected, Errors: errors}, nil
}

func (s *ScannerService) List(ctx context.Context) ([]Market, error) {
	return s.store.ListMarkets(ctx)
}

func (s *ScannerService) baseTradable(m Market, now time.Time) bool {
	if !m.Active || m.Closed || m.YesTokenID == "" {
		return false
	}
	if m.Volume < s.cfg.MarketMinVolumeUSD || m.Liquidity < s.cfg.MarketMinLiquidityUSD {
		return false
	}
	if !m.EndTime.IsZero() && m.EndTime.Sub(now).Hours() < s.cfg.MarketMinHoursToExpiry {
		return false
	}
	return true
}

func (s *ScannerService) afterEnrichmentTradable(m Market) bool {
	if m.Spread > 0 && m.Spread > s.cfg.MarketMaxSpread {
		return false
	}
	if m.BestAsk <= 0 || m.BestAsk >= 1 {
		return false
	}
	return true
}
