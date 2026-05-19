package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	marketscanner "github.com/local/polymarket-fetch-data-service/app/marketScanner"
	marketstream "github.com/local/polymarket-fetch-data-service/app/marketStream"

	newsfetcher "github.com/local/polymarket-fetch-data-service/app/newsFetcher"
	"github.com/local/polymarket-fetch-data-service/pkg/config"
	"github.com/local/polymarket-fetch-data-service/pkg/eventbus"
	"github.com/local/polymarket-fetch-data-service/pkg/logger"
	appruntime "github.com/local/polymarket-fetch-data-service/pkg/runtime"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)
	log.Info("fetchData_service_starting", "config", cfg.RedactedFields())
	if err := appruntime.ConnectKafka(ctx, cfg, log, "fetchData"); err != nil {
		log.Error("kafka_connect_failed", "error", err)
		os.Exit(1)
	}
	kafkaProducer, err := appruntime.OpenKafkaProducer(cfg, log, "polymarket-fetch-data-service")
	if err != nil {
		log.Error("kafka_producer_failed", "error", err)
		os.Exit(1)
	}
	defer kafkaProducer.Close()
	store := appruntime.OpenStore(ctx, cfg, log)
	defer store.Close()
	appCache := appruntime.OpenCache(ctx, cfg, log)
	defer appCache.Close()

	var bus eventbus.Bus = eventbus.NewInMemory()
	if kafkaProducer != nil {
		bus = eventbus.NewKafkaBus(kafkaProducer, log)
	}
	polyREST := marketscanner.NewPolymarketRESTClient(cfg.PolymarketGammaBaseURL, cfg.PolymarketCLOBBaseURL, 30*time.Second)
	polyWS := marketstream.NewPolymarketWSClient(cfg.PolymarketWSMarketURL, log)
	gdelt := newsfetcher.NewHTTPGDELTClient(cfg.NewsGDELTBaseURL, 30*time.Second)
	rss := newsfetcher.NewFeedRSSClient()
	scanner := marketscanner.NewScannerService(cfg, polyREST, store, log)
	stream := marketstream.NewStreamService(polyWS, store, appCache, log)
	news := newsfetcher.NewFetcherService(cfg, gdelt, rss, store, appCache, log)
	scanner.SetEventBus(bus)
	stream.SetEventBus(bus)
	news.SetEventBus(bus)

	runFetchDataLoop(ctx, cfg, log, scanner, stream, news)
}

func runFetchDataLoop(ctx context.Context, cfg config.Config, log *slog.Logger, scanner *marketscanner.ScannerService, stream *marketstream.StreamService, news *newsfetcher.FetcherService) {
	refreshMarkets(ctx, log, scanner, stream)
	fetchNews(ctx, log, news)
	ticker := time.NewTicker(cfg.NewsPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			stream.Stop()
			log.Info("fetchData_service_stopped")
			return
		case <-ticker.C:
			refreshMarkets(ctx, log, scanner, stream)
			fetchNews(ctx, log, news)
		}
	}
}

func refreshMarkets(ctx context.Context, log *slog.Logger, scanner *marketscanner.ScannerService, stream *marketstream.StreamService) {
	result, err := scanner.Refresh(ctx)
	if err != nil {
		log.Warn("market_refresh_failed", "error", err)
		return
	}
	log.Info("market_refresh_completed", "selected", len(result.Selected), "errors", len(result.Errors))
	if err := stream.Start(ctx); err != nil {
		log.Warn("market_stream_start_failed", "error", err)
	}
}

func fetchNews(ctx context.Context, log *slog.Logger, news *newsfetcher.FetcherService) {
	result, err := news.FetchNow(ctx)
	if err != nil {
		log.Warn("news_fetch_failed", "error", err)
		return
	}
	log.Info("news_fetch_completed", "articles", len(result.Articles), "errors", len(result.Errors))
}
