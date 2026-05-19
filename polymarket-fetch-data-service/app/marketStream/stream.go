package marketstream

import (
	"context"
	"github.com/local/polymarket-fetch-data-service/pkg/cache"
	"github.com/local/polymarket-fetch-data-service/pkg/eventbus"
	"github.com/local/polymarket-fetch-data-service/pkg/idgen"
	"github.com/local/polymarket-fetch-data-service/pkg/repository"
	"log/slog"
	"strings"
	"sync"
	"time"
)

type StreamService struct {
	ws      WSClient
	store   repository.Store
	cache   cache.Cache
	log     *slog.Logger
	bus     eventbus.Bus
	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool
}

func NewStreamService(ws WSClient, store repository.Store, cache cache.Cache, log *slog.Logger) *StreamService {
	return &StreamService{ws: ws, store: store, cache: cache, log: log}
}

func (s *StreamService) SetEventBus(bus eventbus.Bus) {
	s.bus = bus
}

func (s *StreamService) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return nil
	}
	markets, err := s.store.ListMarkets(ctx)
	if err != nil {
		return err
	}
	assetIDs := []string{}
	assetToMarketID := map[string]string{}
	for _, m := range markets {
		if m.YesTokenID != "" {
			assetIDs = append(assetIDs, m.YesTokenID)
			assetToMarketID[m.YesTokenID] = m.ID
		}
		if m.NoTokenID != "" {
			assetToMarketID[m.NoTokenID] = m.ID
		}
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.running = true
	events := make(chan MarketEvent, 128)
	go func() {
		defer func() {
			cancel()
			close(events)
			s.mu.Lock()
			s.running = false
			s.mu.Unlock()
		}()
		if err := s.ws.Run(streamCtx, assetIDs, events); err != nil && streamCtx.Err() == nil {
			s.log.Warn("market stream stopped", "error", err)
		}
	}()
	go s.consume(streamCtx, events, assetToMarketID)
	return nil
}

func (s *StreamService) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	s.running = false
}

func (s *StreamService) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

func (s *StreamService) consume(ctx context.Context, events <-chan MarketEvent, assetToMarketID map[string]string) {
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return
			}
			st := ev.State
			if st.UpdatedAt.IsZero() {
				st.UpdatedAt = time.Now().UTC()
			}
			if st.MarketID == "" {
				st.MarketID = assetToMarketID[st.AssetID]
			}
			if st.MarketID == "" {
				continue
			}
			previous, hasPrevious := s.previousState(ctx, st.MarketID)
			_ = s.store.SaveLiveMarketState(ctx, st)
			_ = s.cache.SetJSON(ctx, "live_market_state:"+st.MarketID, st, 5*time.Minute)
			if st.AssetID != "" && (len(st.OrderBookBids) > 0 || len(st.OrderBookAsks) > 0) {
				_ = s.cache.SetJSON(ctx, "orderbook:"+st.AssetID, OrderBook{Bids: st.OrderBookBids, Asks: st.OrderBookAsks}, 5*time.Minute)
			}
			_ = s.store.SaveAudit(ctx, repository.AuditLog{ID: idgen.New(), Event: "market_stream_event", EntityID: st.MarketID, Payload: map[string]any{"event_type": ev.EventType, "asset_id": ev.AssetID}, CreatedAt: time.Now().UTC()})
			if s.bus != nil {
				s.bus.Publish(ctx, eventbus.Event{Topic: "market.price.updated", Data: st})
				if len(st.OrderBookBids) > 0 || len(st.OrderBookAsks) > 0 {
					s.bus.Publish(ctx, eventbus.Event{Topic: "market.orderbook.updated", Data: st})
				}
				if strings.Contains(strings.ToLower(ev.EventType), "trade") {
					s.bus.Publish(ctx, eventbus.Event{Topic: "market.trade.executed", Data: ev})
				}
			}
			if hasPrevious && unusualMove(previous, st) {
				_ = s.store.SaveAudit(ctx, repository.AuditLog{
					ID:       idgen.New(),
					Event:    "market_moved",
					EntityID: st.MarketID,
					Payload: map[string]any{
						"asset_id":     st.AssetID,
						"old_mid":      previous.MidPrice,
						"new_mid":      st.MidPrice,
						"old_spread":   previous.Spread,
						"new_spread":   st.Spread,
						"event_type":   ev.EventType,
						"source":       "websocket",
						"trigger_hint": "review related news before AI analysis",
					},
					CreatedAt: time.Now().UTC(),
				})
				if s.bus != nil {
					s.bus.Publish(ctx, eventbus.Event{Topic: "market.moved", Data: st})
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *StreamService) previousState(ctx context.Context, marketID string) (LiveMarketState, bool) {
	previous, err := s.store.GetLiveMarketState(ctx, marketID)
	return previous, err == nil
}

func unusualMove(previous, next LiveMarketState) bool {
	if previous.MarketID == "" || next.MarketID == "" {
		return false
	}
	if previous.MidPrice > 0 && next.MidPrice > 0 && abs(next.MidPrice-previous.MidPrice) >= 0.05 {
		return true
	}
	if previous.Spread > 0 && next.Spread > 0 && next.Spread >= previous.Spread*2 && next.Spread >= 0.03 {
		return true
	}
	return false
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
