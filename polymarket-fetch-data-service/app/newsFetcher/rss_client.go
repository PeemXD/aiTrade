package newsfetcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/mmcdole/gofeed"
	"time"
)

type RSSClient interface {
	Fetch(context.Context, []string) ([]NewsArticle, error)
}

type FeedRSSClient struct {
	parser *gofeed.Parser
}

func NewFeedRSSClient() *FeedRSSClient {
	return &FeedRSSClient{parser: gofeed.NewParser()}
}

func (c *FeedRSSClient) Fetch(ctx context.Context, feeds []string) ([]NewsArticle, error) {
	out := []NewsArticle{}
	now := time.Now().UTC()
	var firstErr error
	for _, feedURL := range feeds {
		feed, err := c.parser.ParseURLWithContext(feedURL, ctx)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, item := range feed.Items {
			published := now
			if item.PublishedParsed != nil {
				published = item.PublishedParsed.UTC()
			}
			source := feed.Title
			if source == "" {
				source = feedURL
			}
			content := item.Content
			if content == "" {
				content = item.Description
			}
			hash := rssHash(item.Link, item.Title, source, published)
			out = append(out, NewsArticle{
				ID: titleID(hash), Source: "rss:" + source, Title: item.Title, URL: item.Link,
				Content: content, Summary: item.Description, PublishedAt: published, FetchedAt: now, Hash: hash,
			})
		}
	}
	return out, firstErr
}

func rssHash(parts ...any) string {
	h := sha256.Sum256([]byte(fmt.Sprint(parts...)))
	return hex.EncodeToString(h[:])
}
