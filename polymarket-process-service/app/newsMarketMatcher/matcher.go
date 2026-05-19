package newsmarketmatcher

import (
	"sort"
	"strings"
	"time"

	"github.com/local/polymarket-process-service/pkg/idgen"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	matchmodel "github.com/local/polymarket-process-service/pkg/model/match"
	newsmodel "github.com/local/polymarket-process-service/pkg/model/news"
	"github.com/local/polymarket-process-service/pkg/textmatch"
)

type MatcherService struct {
	threshold float64
	topN      int
}

func NewMatcherService(threshold float64, topN int) *MatcherService {
	if threshold <= 0 {
		threshold = 0.70
	}
	if topN <= 0 {
		topN = 3
	}
	return &MatcherService{threshold: threshold, topN: topN}
}

func (s *MatcherService) Match(news newsmodel.NewsArticle, markets []marketmodel.Market) []matchmodel.NewsMarketMatch {
	newsKeywords := Keywords(news.Title + " " + news.Content + " " + strings.Join(news.Keywords, " "))
	newsEntities := Entities(news.Title + " " + news.Content + " " + strings.Join(news.Entities, " "))
	matches := []matchmodel.NewsMarketMatch{}
	for _, market := range markets {
		marketKeywords := Keywords(market.Question)
		marketEntities := Entities(market.Question)
		keywordScore := overlapScore(newsKeywords, marketKeywords)
		entityScore := overlapScore(newsEntities, marketEntities)
		embeddingScore := 0.0
		final := 0.45*keywordScore + 0.40*entityScore + 0.15*embeddingScore
		if final >= s.threshold {
			matches = append(matches, matchmodel.NewsMarketMatch{
				ID: idgen.New(), NewsID: news.ID, MarketID: market.ID,
				KeywordScore: keywordScore, EntityScore: entityScore, EmbeddingScore: embeddingScore,
				FinalScore: final, Reason: reason(newsEntities, marketEntities, newsKeywords, marketKeywords), CreatedAt: time.Now().UTC(),
			})
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].FinalScore > matches[j].FinalScore })
	if len(matches) > s.topN {
		matches = matches[:s.topN]
	}
	return matches
}

func Keywords(text string) []string {
	return textmatch.Keywords(text)
}

func Entities(text string) []string {
	return textmatch.Entities(text)
}

func overlapScore(left, right []string) float64 {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	set := map[string]bool{}
	for _, v := range right {
		set[v] = true
	}
	overlap := 0
	for _, v := range left {
		if set[v] {
			overlap++
		}
	}
	denom := min(len(left), len(right))
	return float64(overlap) / float64(denom)
}

func reason(newsEntities, marketEntities, newsKeywords, marketKeywords []string) string {
	shared := []string{}
	set := map[string]bool{}
	for _, v := range marketEntities {
		set[v] = true
	}
	for _, v := range newsEntities {
		if set[v] {
			shared = append(shared, v)
		}
	}
	if len(shared) == 0 {
		set = map[string]bool{}
		for _, v := range marketKeywords {
			set[v] = true
		}
		for _, v := range newsKeywords {
			if set[v] {
				shared = append(shared, v)
			}
		}
	}
	sort.Strings(shared)
	return "matched " + strings.Join(shared, ", ")
}
