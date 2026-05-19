package model

import "time"

type NewsMarketMatch struct {
	ID             string    `json:"id"`
	NewsID         string    `json:"news_id"`
	MarketID       string    `json:"market_id"`
	KeywordScore   float64   `json:"keyword_score"`
	EntityScore    float64   `json:"entity_score"`
	EmbeddingScore float64   `json:"embedding_score"`
	FinalScore     float64   `json:"final_score"`
	Reason         string    `json:"reason"`
	CreatedAt      time.Time `json:"created_at"`
}
