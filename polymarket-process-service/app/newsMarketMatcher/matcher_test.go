package newsmarketmatcher

import (
	"testing"
	"time"

	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	newsmodel "github.com/local/polymarket-process-service/pkg/model/news"
	"github.com/stretchr/testify/require"
)

func TestMatcherRelatedETHETFNewsMatchesMarket(t *testing.T) {
	matcher := NewMatcherService(0.70, 3)
	news := newsmodel.NewsArticle{ID: "n1", Title: "SEC delays decision on spot Ethereum ETF", Content: "The SEC delayed a spot Ethereum ETF decision.", PublishedAt: time.Now()}
	markets := []marketmodel.Market{{ID: "m1", Question: "Will Ethereum ETF be approved before June 30?", Category: "crypto"}}

	matches := matcher.Match(news, markets)

	require.Len(t, matches, 1)
	require.Equal(t, "m1", matches[0].MarketID)
	require.GreaterOrEqual(t, matches[0].FinalScore, 0.70)
}

func TestMatcherUnrelatedNewsIgnored(t *testing.T) {
	matcher := NewMatcherService(0.70, 3)
	news := newsmodel.NewsArticle{ID: "n1", Title: "NBA player injury update", Content: "A guard is questionable for tonight."}
	markets := []marketmodel.Market{{ID: "m1", Question: "Will ETH ETF be approved?", Category: "crypto"}}

	matches := matcher.Match(news, markets)

	require.Empty(t, matches)
}

func TestMatcherSynonymMatchingWorks(t *testing.T) {
	matcher := NewMatcherService(0.70, 3)
	news := newsmodel.NewsArticle{ID: "n1", Title: "Ether rallies after ETF update", Content: "ETH traders reacted to ETF news."}
	markets := []marketmodel.Market{{ID: "m1", Question: "Will Ethereum ETF be approved?", Category: "crypto"}}

	matches := matcher.Match(news, markets)

	require.Len(t, matches, 1)
	require.Equal(t, "m1", matches[0].MarketID)
}

func TestMatcherThresholdFiltersWeakMatches(t *testing.T) {
	matcher := NewMatcherService(0.90, 3)
	news := newsmodel.NewsArticle{ID: "n1", Title: "SEC comments on crypto enforcement", Content: "Agency officials discussed compliance."}
	markets := []marketmodel.Market{{ID: "m1", Question: "Will Bitcoin hit a new all time high?", Category: "crypto"}}

	matches := matcher.Match(news, markets)

	require.Empty(t, matches)
}
