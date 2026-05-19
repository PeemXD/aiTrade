package model

import "time"

type OrderRequest struct {
	RiskDecisionID string  `json:"risk_decision_id"`
	MarketID       string  `json:"market_id"`
	Outcome        string  `json:"outcome"`
	Side           string  `json:"side"`
	LimitPrice     float64 `json:"limit_price"`
	SizeUSD        float64 `json:"size_usd"`
	Reason         string  `json:"reason"`
	IdempotencyKey string  `json:"idempotency_key,omitempty"`
}

type OrderResult struct {
	TradeID       string  `json:"trade_id"`
	Status        string  `json:"status"`
	AveragePrice  float64 `json:"average_price"`
	Quantity      float64 `json:"quantity"`
	FilledSizeUSD float64 `json:"filled_size_usd"`
	FeesUSD       float64 `json:"fees_usd"`
	SlippageUSD   float64 `json:"slippage_usd"`
	Message       string  `json:"message"`
}

type Trade struct {
	ID               string     `json:"id"`
	Mode             string     `json:"mode"`
	MarketID         string     `json:"market_id"`
	Outcome          string     `json:"outcome"`
	Side             string     `json:"side"`
	Status           string     `json:"status"`
	EntryPrice       float64    `json:"entry_price"`
	ExitPrice        float64    `json:"exit_price"`
	SizeUSD          float64    `json:"size_usd"`
	Quantity         float64    `json:"quantity"`
	FeesUSD          float64    `json:"fees_usd"`
	SlippageUSD      float64    `json:"slippage_usd"`
	RealizedPnLUSD   float64    `json:"realized_pnl_usd"`
	UnrealizedPnLUSD float64    `json:"unrealized_pnl_usd"`
	Reason           string     `json:"reason"`
	OpenedAt         *time.Time `json:"opened_at,omitempty"`
	ClosedAt         *time.Time `json:"closed_at,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}
