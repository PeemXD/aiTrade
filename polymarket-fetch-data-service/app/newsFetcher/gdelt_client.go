package newsfetcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/local/polymarket-fetch-data-service/pkg/httpclient"
)

type GDELTClient interface {
	Fetch(context.Context, string, int) ([]NewsArticle, error)
}

type HTTPGDELTClient struct {
	baseURL string
	client  *httpclient.Client
}

func NewHTTPGDELTClient(baseURL string, timeout time.Duration) *HTTPGDELTClient {
	return &HTTPGDELTClient{baseURL: baseURL, client: httpclient.New(timeout)}
}

func (c *HTTPGDELTClient) Fetch(ctx context.Context, query string, limit int) ([]NewsArticle, error) {
	if limit <= 0 {
		limit = 25
	}
	values := url.Values{}
	values.Set("query", query)
	values.Set("mode", "ArtList")
	values.Set("format", "json")
	values.Set("maxrecords", fmt.Sprint(limit))
	values.Set("sort", "datedesc")
	values.Set("timespan", "24h")
	var raw struct {
		Articles []struct {
			URL           string `json:"url"`
			Title         string `json:"title"`
			SeenDate      string `json:"seendate"`
			Domain        string `json:"domain"`
			Language      string `json:"language"`
			SourceCountry string `json:"sourcecountry"`
		} `json:"articles"`
	}
	if err := c.client.GetJSON(ctx, c.baseURL+"?"+values.Encode(), nil, &raw); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := make([]NewsArticle, 0, len(raw.Articles))
	for _, a := range raw.Articles {
		published := parseGDELTTime(a.SeenDate)
		title := strings.TrimSpace(a.Title)
		hash := articleHash(a.URL, title, a.Domain, published)
		out = append(out, NewsArticle{
			ID: titleID(hash), Source: "gdelt:" + a.Domain, Title: title, URL: a.URL,
			Content: title, Summary: title, PublishedAt: published, FetchedAt: now, Hash: hash,
		})
	}
	return out, nil
}

func parseGDELTTime(raw string) time.Time {
	for _, layout := range []string{"20060102150405", "20060102T150405Z", time.RFC3339} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC()
		}
	}
	return time.Now().UTC()
}

func articleHash(parts ...any) string {
	h := sha256.Sum256([]byte(fmt.Sprint(parts...)))
	return hex.EncodeToString(h[:])
}

func titleID(hash string) string {
	if len(hash) > 32 {
		return hash[:32]
	}
	return hash
}
