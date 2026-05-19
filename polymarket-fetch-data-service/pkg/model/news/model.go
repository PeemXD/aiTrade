package model

import "time"

type NewsArticle struct {
	ID          string    `json:"id"`
	Source      string    `json:"source"`
	Title       string    `json:"title"`
	URL         string    `json:"url"`
	Content     string    `json:"content"`
	Summary     string    `json:"summary"`
	PublishedAt time.Time `json:"published_at"`
	FetchedAt   time.Time `json:"fetched_at"`
	Hash        string    `json:"hash"`
	Entities    []string  `json:"entities"`
	Keywords    []string  `json:"keywords"`
}

type FetchResult struct {
	Articles []NewsArticle `json:"articles"`
	Errors   []string      `json:"errors,omitempty"`
}
