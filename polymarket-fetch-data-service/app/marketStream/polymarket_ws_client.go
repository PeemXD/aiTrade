package marketstream

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
)

type MarketEvent struct {
	EventType string
	AssetID   string
	MarketID  string
	State     LiveMarketState
	Raw       map[string]any
}

type WSClient interface {
	Run(context.Context, []string, chan<- MarketEvent) error
}

type PolymarketWSClient struct {
	url string
	log *slog.Logger
}

func NewPolymarketWSClient(url string, log *slog.Logger) *PolymarketWSClient {
	return &PolymarketWSClient{url: url, log: log}
}

func (c *PolymarketWSClient) Run(ctx context.Context, assetIDs []string, events chan<- MarketEvent) error {
	backoff := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.url, nil)
		if err != nil {
			c.log.Warn("market websocket dial failed", "error", err)
			select {
			case <-time.After(backoff):
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		stopCloseOnCancel := closeOnContextDone(ctx, conn)
		backoff = time.Second
		sub := map[string]any{"assets_ids": assetIDs, "type": "market", "custom_feature_enabled": true}
		if err := conn.WriteJSON(sub); err != nil {
			stopCloseOnCancel()
			_ = conn.Close()
			return err
		}
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				stopCloseOnCancel()
				_ = conn.Close()
				break
			}
			ev := parseMarketEvent(data)
			select {
			case events <- ev:
			case <-ctx.Done():
				stopCloseOnCancel()
				_ = conn.Close()
				return ctx.Err()
			}
		}
	}
}

func closeOnContextDone(ctx context.Context, conn *websocket.Conn) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	return func() { close(done) }
}

func parseMarketEvent(data []byte) MarketEvent {
	var raw map[string]any
	_ = json.Unmarshal(data, &raw)
	eventType, _ := raw["event_type"].(string)
	assetID, _ := raw["asset_id"].(string)
	marketID, _ := raw["market"].(string)
	state := LiveMarketState{
		MarketID:  marketID,
		AssetID:   assetID,
		BestBid:   anyFloat(raw["best_bid"]),
		BestAsk:   anyFloat(raw["best_ask"]),
		LastPrice: anyFloat(raw["price"]),
		UpdatedAt: time.Now().UTC(),
	}
	if eventType == "book" {
		if bids, ok := raw["bids"].([]any); ok {
			state.OrderBookBids = levelsFromAny(bids)
		}
		if asks, ok := raw["asks"].([]any); ok {
			state.OrderBookAsks = levelsFromAny(asks)
		}
		if len(state.OrderBookBids) > 0 {
			state.BestBid = state.OrderBookBids[0].Price
		}
		if len(state.OrderBookAsks) > 0 {
			state.BestAsk = state.OrderBookAsks[0].Price
		}
	}
	if state.BestBid > 0 && state.BestAsk > 0 {
		state.MidPrice = (state.BestBid + state.BestAsk) / 2
		state.Spread = state.BestAsk - state.BestBid
	}
	return MarketEvent{EventType: eventType, AssetID: assetID, MarketID: marketID, State: state, Raw: raw}
}

func levelsFromAny(items []any) []OrderBookLevel {
	out := make([]OrderBookLevel, 0, len(items))
	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			out = append(out, OrderBookLevel{Price: anyFloat(m["price"]), Size: anyFloat(m["size"])})
		}
	}
	return out
}
