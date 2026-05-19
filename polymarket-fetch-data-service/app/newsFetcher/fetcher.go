package newsfetcher

import (
	"context"
	"github.com/local/polymarket-fetch-data-service/pkg/cache"
	"github.com/local/polymarket-fetch-data-service/pkg/config"
	"github.com/local/polymarket-fetch-data-service/pkg/eventbus"
	"github.com/local/polymarket-fetch-data-service/pkg/idgen"
	"github.com/local/polymarket-fetch-data-service/pkg/repository"
	"github.com/local/polymarket-fetch-data-service/pkg/textmatch"
	"log/slog"
	"time"
)

type FetcherService struct {
	cfg    config.Config
	gdelt  GDELTClient
	rss    RSSClient
	store  repository.Store
	cache  cache.Cache
	logger *slog.Logger
	bus    eventbus.Bus
}

func NewFetcherService(cfg config.Config, gdelt GDELTClient, rss RSSClient, store repository.Store, cache cache.Cache, logger *slog.Logger) *FetcherService {
	return &FetcherService{cfg: cfg, gdelt: gdelt, rss: rss, store: store, cache: cache, logger: logger}
}

func (s *FetcherService) SetEventBus(bus eventbus.Bus) {
	s.bus = bus
}

func (s *FetcherService) FetchNow(ctx context.Context) (FetchResult, error) {
	var all []NewsArticle
	errors := []string{}
	if s.gdelt != nil {
		articles, err := s.gdelt.Fetch(ctx, s.cfg.NewsGDELTQuery, 50)
		if err != nil {
			errors = append(errors, "gdelt: "+err.Error())
		}
		all = append(all, articles...)
	}
	if s.rss != nil {
		articles, err := s.rss.Fetch(ctx, s.cfg.NewsRSSFeeds)
		if err != nil {
			errors = append(errors, "rss: "+err.Error())
		}
		all = append(all, articles...)
	}
	unique := make([]NewsArticle, 0, len(all))
	for _, a := range all {
		if a.ID == "" {
			a.ID = idgen.New()
		}
		if a.FetchedAt.IsZero() {
			a.FetchedAt = time.Now().UTC()
		}
		if a.Hash == "" {
			a.Hash = a.ID
		}
		a.Keywords = textmatch.Keywords(a.Title + " " + a.Content)
		a.Entities = textmatch.Entities(a.Title + " " + a.Content)
		ok, err := s.cache.SetNX(ctx, "dedupe:news:"+a.Hash, "1", 24*time.Hour)
		if err != nil || ok {
			unique = append(unique, a)
			continue
		}
		_ = s.store.SaveAudit(ctx, repository.AuditLog{ID: idgen.New(), Event: "news_deduplicated", EntityID: a.ID, Payload: map[string]any{"source": a.Source, "title": a.Title, "hash": a.Hash}, CreatedAt: time.Now().UTC()})
		if s.bus != nil {
			s.bus.Publish(ctx, eventbus.Event{Topic: "news.deduplicated", Data: a})
		}
	}
	if err := s.store.SaveNewsArticles(ctx, unique); err != nil {
		return FetchResult{Articles: unique, Errors: append(errors, err.Error())}, err
	}
	for _, a := range unique {
		_ = s.store.SaveAudit(ctx, repository.AuditLog{ID: idgen.New(), Event: "news_fetched", EntityID: a.ID, Payload: map[string]any{"source": a.Source, "title": a.Title}, CreatedAt: time.Now().UTC()})
		if s.bus != nil {
			s.bus.Publish(ctx, eventbus.Event{Topic: "news.arrived", Data: a})
		}
	}
	if s.bus != nil {
		s.bus.Publish(ctx, eventbus.Event{Topic: "news.fetch.completed", Data: FetchResult{Articles: unique, Errors: errors}})
	}
	s.logger.Info("news_fetch_completed", "articles", len(unique), "errors", len(errors))
	return FetchResult{Articles: unique, Errors: errors}, nil
}

func (s *FetcherService) List(ctx context.Context, limit int) ([]NewsArticle, error) {
	return s.store.ListNewsArticles(ctx, limit)
}
