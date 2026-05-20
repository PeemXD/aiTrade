package marketstream

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/local/polymarket-fetch-data-service/pkg/cache"
	"github.com/local/polymarket-fetch-data-service/pkg/eventbus"
	marketmodel "github.com/local/polymarket-fetch-data-service/pkg/model/market"
	"github.com/local/polymarket-fetch-data-service/pkg/repository"
	"github.com/stretchr/testify/require"
)

func TestConsumeSkipsUnmappedMarketID(t *testing.T) {
	ctx := context.Background()
	store := repository.NewMemoryStore()
	bus := &captureBus{}
	service := newTestStreamService(store, bus)

	events := make(chan MarketEvent, 1)
	events <- MarketEvent{
		EventType: "price_change",
		State: LiveMarketState{
			MarketID:  "0xcondition",
			UpdatedAt: time.Now().UTC(),
		},
	}
	close(events)

	service.consume(ctx, events, map[string]string{"yes-token": "local-market"}, map[string]struct{}{"local-market": {}})

	_, err := store.GetLiveMarketState(ctx, "0xcondition")
	require.Error(t, err)
	require.Empty(t, bus.events)
}

func TestConsumeMapsAssetIDToSelectedMarketID(t *testing.T) {
	ctx := context.Background()
	store := repository.NewMemoryStore()
	require.NoError(t, store.SaveMarkets(ctx, []marketmodel.Market{{
		ID:         "local-market",
		Question:   "Will this market resolve yes?",
		YesTokenID: "yes-token",
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}}))
	bus := &captureBus{}
	service := newTestStreamService(store, bus)

	events := make(chan MarketEvent, 1)
	events <- MarketEvent{
		EventType: "price_change",
		State: LiveMarketState{
			MarketID:  "0xcondition",
			AssetID:   "yes-token",
			BestBid:   0.40,
			BestAsk:   0.42,
			MidPrice:  0.41,
			Spread:    0.02,
			UpdatedAt: time.Now().UTC(),
		},
	}
	close(events)

	service.consume(ctx, events, map[string]string{"yes-token": "local-market"}, map[string]struct{}{"local-market": {}})

	state, err := store.GetLiveMarketState(ctx, "local-market")
	require.NoError(t, err)
	require.Equal(t, "local-market", state.MarketID)
	require.Equal(t, "yes-token", state.AssetID)
	require.Len(t, bus.events, 1)
	require.Equal(t, "market.price.updated", bus.events[0].Topic)
}

func newTestStreamService(store repository.Store, bus eventbus.Bus) *StreamService {
	return &StreamService{
		store: store,
		cache: cache.NewMemory(),
		log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		bus:   bus,
	}
}

type captureBus struct {
	mu     sync.Mutex
	events []eventbus.Event
}

func (b *captureBus) Publish(_ context.Context, event eventbus.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, event)
}

func (b *captureBus) Subscribe(string, eventbus.Handler) {}
