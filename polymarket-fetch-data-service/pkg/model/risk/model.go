package model

import "time"

type EvaluateRequest struct {
	ProbabilityDecisionID string  `json:"probability_decision_id"`
	MarketID              string  `json:"market_id,omitempty"`
	RequestedSizeUSD      float64 `json:"requested_size_usd,omitempty"`
}

type PortfolioState struct {
	CashUSD           float64 `json:"cash_usd"`
	PortfolioValueUSD float64 `json:"portfolio_value_usd"`
	ExposureUSD       float64 `json:"exposure_usd"`
	OpenPnLUSD        float64 `json:"open_pnl_usd"`
	RealizedPnLUSD    float64 `json:"realized_pnl_usd"`
	DailyPnLUSD       float64 `json:"daily_pnl_usd"`
	DailyPnLPct       float64 `json:"daily_pnl_pct"`
	OpenPositions     int     `json:"open_positions"`
}

type RiskDecision struct {
	ID                    string         `json:"id"`
	MarketID              string         `json:"market_id"`
	ProbabilityDecisionID string         `json:"probability_decision_id"`
	Approved              bool           `json:"approved"`
	PositionSizeUSD       float64        `json:"position_size_usd"`
	MaxLossUSD            float64        `json:"max_loss_usd"`
	Checks                map[string]any `json:"checks_json"`
	RejectReason          string         `json:"reject_reason,omitempty"`
	Reason                string         `json:"reason"`
	CreatedAt             time.Time      `json:"created_at"`
}
