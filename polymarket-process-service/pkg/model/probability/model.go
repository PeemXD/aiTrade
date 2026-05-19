package model

import "time"

type Decision string

const (
	DecisionNoTrade   Decision = "no_trade"
	DecisionCandidate Decision = "candidate"
)

type CalculateRequest struct {
	MarketID string `json:"market_id"`
	NewsID   string `json:"news_id,omitempty"`
	SignalID string `json:"signal_id,omitempty"`
	Side     string `json:"side,omitempty"`
	Outcome  string `json:"outcome,omitempty"`
}

type ProbabilityDecision struct {
	ID                string         `json:"id"`
	MarketID          string         `json:"market_id"`
	NewsID            string         `json:"news_id,omitempty"`
	MarketProbability float64        `json:"market_probability"`
	ExecutablePrice   float64        `json:"executable_price"`
	OurProbability    float64        `json:"our_probability"`
	Edge              float64        `json:"edge"`
	Confidence        float64        `json:"confidence"`
	Components        map[string]any `json:"components_json"`
	Decision          Decision       `json:"decision"`
	Reason            string         `json:"reason"`
	Side              string         `json:"side"`
	Outcome           string         `json:"outcome"`
	CreatedAt         time.Time      `json:"created_at"`
}
