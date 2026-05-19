package model

import "time"

type Position struct {
	ID               string    `json:"id"`
	MarketID         string    `json:"market_id"`
	Outcome          string    `json:"outcome"`
	Quantity         float64   `json:"quantity"`
	AvgEntryPrice    float64   `json:"avg_entry_price"`
	CurrentPrice     float64   `json:"current_price"`
	ExposureUSD      float64   `json:"exposure_usd"`
	UnrealizedPnLUSD float64   `json:"unrealized_pnl_usd"`
	Status           string    `json:"status"`
	OpenedAt         time.Time `json:"opened_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type ExitDecision struct {
	ShouldExit bool    `json:"should_exit"`
	PositionID string  `json:"position_id,omitempty"`
	MarketID   string  `json:"market_id,omitempty"`
	Reason     string  `json:"reason"`
	PnLPct     float64 `json:"pnl_pct"`
	Edge       float64 `json:"edge"`
}
