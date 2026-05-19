package model

import "time"

type OrderBookLevel struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
}

type OrderBook struct {
	Bids []OrderBookLevel `json:"bids"`
	Asks []OrderBookLevel `json:"asks"`
}

type Market struct {
	ID          string    `json:"id"`
	ConditionID string    `json:"condition_id"`
	Question    string    `json:"question"`
	Slug        string    `json:"slug"`
	Category    string    `json:"category"`
	Active      bool      `json:"active"`
	Closed      bool      `json:"closed"`
	EndTime     time.Time `json:"end_time"`
	Volume      float64   `json:"volume"`
	Liquidity   float64   `json:"liquidity"`
	YesTokenID  string    `json:"yes_token_id"`
	NoTokenID   string    `json:"no_token_id"`
	YesPrice    float64   `json:"yes_price"`
	NoPrice     float64   `json:"no_price"`
	BestBid     float64   `json:"best_bid"`
	BestAsk     float64   `json:"best_ask"`
	Spread      float64   `json:"spread"`
	OrderBook   OrderBook `json:"orderbook,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type LiveMarketState struct {
	MarketID      string           `json:"market_id"`
	AssetID       string           `json:"asset_id"`
	BestBid       float64          `json:"best_bid"`
	BestAsk       float64          `json:"best_ask"`
	LastPrice     float64          `json:"last_price"`
	MidPrice      float64          `json:"mid_price"`
	Spread        float64          `json:"spread"`
	OrderBookBids []OrderBookLevel `json:"orderbook_bids"`
	OrderBookAsks []OrderBookLevel `json:"orderbook_asks"`
	UpdatedAt     time.Time        `json:"updated_at"`
}

type RefreshResult struct {
	Selected []Market `json:"selected"`
	Errors   []string `json:"errors,omitempty"`
}
