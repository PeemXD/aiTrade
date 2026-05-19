package marketscanner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/local/polymarket-fetch-data-service/pkg/httpclient"
)

type RESTClient interface {
	FetchMarkets(context.Context, int) ([]Market, error)
	GetOrderBook(context.Context, string) (OrderBook, error)
	GetMidpoint(context.Context, string) (float64, error)
	GetSpread(context.Context, string) (float64, error)
}

type PolymarketRESTClient struct {
	gammaBase string
	clobBase  string
	client    *httpclient.Client
}

func NewPolymarketRESTClient(gammaBase, clobBase string, timeout time.Duration) *PolymarketRESTClient {
	return &PolymarketRESTClient{gammaBase: strings.TrimRight(gammaBase, "/"), clobBase: strings.TrimRight(clobBase, "/"), client: httpclient.New(timeout)}
}

func (c *PolymarketRESTClient) FetchMarkets(ctx context.Context, limit int) ([]Market, error) {
	if limit <= 0 {
		limit = 100
	}
	u := fmt.Sprintf("%s/markets?active=true&closed=false&limit=%d", c.gammaBase, limit)
	var raw []map[string]any
	if err := c.client.GetJSON(ctx, u, nil, &raw); err != nil {
		return nil, err
	}
	out := make([]Market, 0, len(raw))
	now := time.Now().UTC()
	for _, item := range raw {
		m := Market{
			ID:          firstString(item, "id", "market"),
			ConditionID: firstString(item, "conditionId", "condition_id"),
			Question:    firstString(item, "question", "title"),
			Slug:        firstString(item, "slug"),
			Category:    firstCategory(item),
			Active:      firstBool(item, true, "active"),
			Closed:      firstBool(item, false, "closed"),
			EndTime:     parseTimeAny(item["endDate"]),
			Volume:      firstFloat(item, "volume", "volumeNum", "volume24hr", "volumeClob"),
			Liquidity:   firstFloat(item, "liquidity", "liquidityNum"),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		outcomes := parseStringArray(item["outcomes"])
		prices := parseFloatArray(item["outcomePrices"])
		tokens := parseStringArray(firstAny(item, "clobTokenIds", "clob_token_ids"))
		for i, outcome := range outcomes {
			name := strings.ToLower(outcome)
			if name == "yes" {
				if i < len(tokens) {
					m.YesTokenID = tokens[i]
				}
				if i < len(prices) {
					m.YesPrice = prices[i]
				}
			}
			if name == "no" {
				if i < len(tokens) {
					m.NoTokenID = tokens[i]
				}
				if i < len(prices) {
					m.NoPrice = prices[i]
				}
			}
		}
		if m.YesTokenID == "" && len(tokens) > 0 {
			m.YesTokenID = tokens[0]
		}
		if m.NoTokenID == "" && len(tokens) > 1 {
			m.NoTokenID = tokens[1]
		}
		if m.YesPrice == 0 && len(prices) > 0 {
			m.YesPrice = prices[0]
		}
		if m.NoPrice == 0 && len(prices) > 1 {
			m.NoPrice = prices[1]
		}
		if m.ConditionID == "" {
			m.ConditionID = m.ID
		}
		if m.ID == "" {
			m.ID = m.ConditionID
		}
		out = append(out, m)
	}
	return out, nil
}

func (c *PolymarketRESTClient) GetOrderBook(ctx context.Context, tokenID string) (OrderBook, error) {
	var raw struct {
		Bids []bookLevel `json:"bids"`
		Asks []bookLevel `json:"asks"`
	}
	u := fmt.Sprintf("%s/book?token_id=%s", c.clobBase, url.QueryEscape(tokenID))
	if err := c.client.GetJSON(ctx, u, nil, &raw); err != nil {
		return OrderBook{}, err
	}
	return OrderBook{Bids: convertLevels(raw.Bids), Asks: convertLevels(raw.Asks)}, nil
}

func (c *PolymarketRESTClient) GetMidpoint(ctx context.Context, tokenID string) (float64, error) {
	var raw map[string]any
	u := fmt.Sprintf("%s/midpoint?token_id=%s", c.clobBase, url.QueryEscape(tokenID))
	if err := c.client.GetJSON(ctx, u, nil, &raw); err != nil {
		return 0, err
	}
	return anyFloat(firstAny(raw, "mid", "midpoint")), nil
}

func (c *PolymarketRESTClient) GetSpread(ctx context.Context, tokenID string) (float64, error) {
	var raw map[string]any
	u := fmt.Sprintf("%s/spread?token_id=%s", c.clobBase, url.QueryEscape(tokenID))
	if err := c.client.GetJSON(ctx, u, nil, &raw); err != nil {
		return 0, err
	}
	return anyFloat(firstAny(raw, "spread")), nil
}

type bookLevel struct {
	Price any `json:"price"`
	Size  any `json:"size"`
}

func convertLevels(in []bookLevel) []OrderBookLevel {
	out := make([]OrderBookLevel, 0, len(in))
	for _, v := range in {
		out = append(out, OrderBookLevel{Price: anyFloat(v.Price), Size: anyFloat(v.Size)})
	}
	return out
}

func firstAny(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return v
		}
	}
	return nil
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
			return fmt.Sprint(v)
		}
	}
	return ""
}

func firstBool(m map[string]any, fallback bool, keys ...string) bool {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if b, ok := v.(bool); ok {
				return b
			}
			if s, ok := v.(string); ok {
				return strings.EqualFold(s, "true")
			}
		}
	}
	return fallback
}

func firstFloat(m map[string]any, keys ...string) float64 {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return anyFloat(v)
		}
	}
	return 0
}

func firstCategory(m map[string]any) string {
	if s := firstString(m, "category"); s != "" {
		return s
	}
	tags := parseStringArray(m["tags"])
	if len(tags) > 0 {
		return tags[0]
	}
	return "general"
}

func anyFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f
	default:
		return 0
	}
}

func parseStringArray(v any) []string {
	switch x := v.(type) {
	case []string:
		return x
	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			out = append(out, fmt.Sprint(item))
		}
		return out
	case string:
		x = strings.TrimSpace(x)
		if x == "" {
			return nil
		}
		var arr []string
		if json.Unmarshal([]byte(x), &arr) == nil {
			return arr
		}
		return strings.Split(x, ",")
	default:
		return nil
	}
}

func parseFloatArray(v any) []float64 {
	strs := parseStringArray(v)
	if len(strs) > 0 {
		out := make([]float64, 0, len(strs))
		for _, s := range strs {
			out = append(out, anyFloat(s))
		}
		return out
	}
	if arr, ok := v.([]any); ok {
		out := make([]float64, 0, len(arr))
		for _, item := range arr {
			out = append(out, anyFloat(item))
		}
		return out
	}
	return nil
}

func parseTimeAny(v any) time.Time {
	if s, ok := v.(string); ok && s != "" {
		for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05Z", "2006-01-02 15:04:05"} {
			if t, err := time.Parse(layout, s); err == nil {
				return t.UTC()
			}
		}
	}
	return time.Now().UTC().Add(24 * time.Hour)
}
