package model

import "time"

type Direction string

const (
	DirectionBullish Direction = "bullish"
	DirectionBearish Direction = "bearish"
	DirectionNeutral Direction = "neutral"
)

type AISignal struct {
	ID                string    `json:"id"`
	MarketID          string    `json:"market_id"`
	NewsID            string    `json:"news_id"`
	Related           bool      `json:"related"`
	Direction         Direction `json:"direction"`
	ProbabilityDelta  float64   `json:"probability_delta"`
	Confidence        float64   `json:"confidence"`
	SourceReliability float64   `json:"source_reliability"`
	PricedInRisk      string    `json:"priced_in_risk"`
	Reasoning         string    `json:"reasoning"`
	RawResponse       string    `json:"raw_response"`
	Disabled          bool      `json:"disabled"`
	CreatedAt         time.Time `json:"created_at"`
}

type AnalyzeRequest struct {
	MarketID string `json:"market_id"`
	NewsID   string `json:"news_id"`
}

type AIProviderOutput struct {
	Related           bool    `json:"related"`
	Direction         string  `json:"direction"`
	ProbabilityDelta  float64 `json:"probability_delta"`
	Confidence        float64 `json:"confidence"`
	SourceReliability float64 `json:"source_reliability"`
	Reason            string  `json:"reason"`
	PricedInRisk      string  `json:"priced_in_risk"`
}
