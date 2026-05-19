package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	aisignal "github.com/local/polymarket-process-service/app/aiSignal"
	dashboardhandler "github.com/local/polymarket-process-service/app/dashboard/handler"
	dashboardservice "github.com/local/polymarket-process-service/app/dashboard/service"
	executionengine "github.com/local/polymarket-process-service/app/executionEngine"
	exitengine "github.com/local/polymarket-process-service/app/exitEngine"
	livehandler "github.com/local/polymarket-process-service/app/live/handler"
	liveservice "github.com/local/polymarket-process-service/app/live/service"
	newsmarketmatcher "github.com/local/polymarket-process-service/app/newsMarketMatcher"
	positionengine "github.com/local/polymarket-process-service/app/positionEngine"
	probabilityengine "github.com/local/polymarket-process-service/app/probabilityEngine"
	riskengine "github.com/local/polymarket-process-service/app/riskEngine"
	"github.com/local/polymarket-process-service/pkg/config"
	"github.com/local/polymarket-process-service/pkg/eventbus"
	"github.com/local/polymarket-process-service/pkg/kafka"
	"github.com/local/polymarket-process-service/pkg/logger"
	appruntime "github.com/local/polymarket-process-service/pkg/runtime"
	"github.com/local/polymarket-process-service/router"
)

const realTradingConfirmation = "I_UNDERSTAND_REAL_TRADING"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)
	cfg = enforcePaperMode(cfg, log)
	log.Info("process_service_starting", "config", cfg.RedactedFields())
	if err := appruntime.ConnectKafka(ctx, cfg, log, "process"); err != nil {
		log.Error("kafka_connect_failed", "error", err)
		os.Exit(1)
	}
	kafkaProducer, err := appruntime.OpenKafkaProducer(cfg, log, "polymarket-process-service")
	if err != nil {
		log.Error("kafka_producer_failed", "error", err)
		os.Exit(1)
	}
	defer kafkaProducer.Close()
	store := appruntime.OpenStore(ctx, cfg, log)
	defer store.Close()

	stream := dashboardservice.NewStreamService(cfg.DashboardMarketThrottle)
	bus := buildEventBus(kafkaProducer, stream, log)
	chat := aisignal.NewOpenAICompatibleClient(cfg.AIBaseURL, cfg.AITimeout)
	var executionProvider executionengine.ExecutionProvider = executionengine.NewPaperExecutionProvider(store)
	if cfg.ExecutionMode == "real" {
		executionProvider = executionengine.NewPolymarketExecutionProvider(cfg)
	}
	matcher := newsmarketmatcher.NewMatcherService(cfg.NewsMarketMatchThreshold, cfg.MaxAIMatchesPerArticle)
	ai := aisignal.NewSignalService(cfg, chat, store, log)
	prob := probabilityengine.NewEngine(cfg, store)
	risk := riskengine.NewEngine(cfg, store)
	execution := executionengine.NewService(store, executionProvider)
	monitor := positionengine.NewMonitor(cfg, store)
	exit := exitengine.NewExitEngine(store, execution)
	execution.SetStartingCash(cfg.PaperStartingBalanceUSD)
	for _, setter := range []interface{ SetEventBus(eventbus.Bus) }{ai, prob, risk, execution, monitor, exit} {
		setter.SetEventBus(bus)
	}
	live := liveservice.NewService(liveservice.Dependencies{
		Config: cfg, Store: store, Matcher: matcher, AI: ai, Prob: prob, Risk: risk,
		Execution: execution, Monitor: monitor, Exit: exit, Bus: bus, Log: log,
	})
	consumers := startKafkaConsumers(ctx, cfg, kafkaProducer, live, stream, log)
	defer closeConsumers(consumers)

	dashboardSvc := dashboardservice.NewDashboardService(cfg, store, live)
	dashboardHandler := dashboardhandler.NewDashboardHandler(cfg, store, dashboardSvc)
	sseHandler := dashboardhandler.NewSSEHandler(stream, cfg.DashboardSSEHeartbeat)
	liveHandler := livehandler.NewLiveHandler(live)
	server := &http.Server{
		Addr:              ":" + cfg.AppPort,
		Handler:           router.New(dashboardHandler, sseHandler, liveHandler),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("process_api_starting", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("process_api_failed", "error", err)
			stop()
		}
	}()
	<-ctx.Done()
	live.Stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Warn("process_api_shutdown_failed", "error", err)
	}
	log.Info("process_service_stopped")
}

func enforcePaperMode(cfg config.Config, log *slog.Logger) config.Config {
	if cfg.ExecutionMode != "real" {
		cfg.ExecutionMode = "paper"
		cfg.EnableRealTrading = false
		return cfg
	}
	if cfg.EnableRealTrading && cfg.RealTradingConfirmation == realTradingConfirmation {
		return cfg
	}
	log.Warn("real_trading_not_confirmed_using_paper", "required_confirmation", realTradingConfirmation)
	cfg.ExecutionMode = "paper"
	cfg.EnableRealTrading = false
	return cfg
}

func buildEventBus(producer *kafka.Producer, stream *dashboardservice.StreamService, log *slog.Logger) eventbus.Bus {
	buses := []eventbus.Bus{eventbus.NewInMemory(), dashboardservice.NewEventBusStream(stream)}
	if producer != nil {
		buses = append(buses, eventbus.NewKafkaBus(producer, log))
	}
	return eventbus.NewComposite(buses...)
}

func startKafkaConsumers(ctx context.Context, cfg config.Config, producer *kafka.Producer, live *liveservice.Service, stream *dashboardservice.StreamService, log *slog.Logger) []*kafka.Consumer {
	if !cfg.KafkaEnabled || cfg.EventBus != "kafka" || producer == nil {
		return nil
	}
	kafkaCfg := kafka.NewConfig(cfg.KafkaEnabled, cfg.KafkaBrokers, cfg.KafkaClientID, cfg.KafkaConsumerGroupProcess, cfg.KafkaAutoCreateTopics)
	definitions := []struct {
		group   string
		topics  []string
		handler kafka.HandlerFunc
	}{
		{cfg.KafkaConsumerGroupProcess + "-news", []string{kafka.TopicNewsArrived, kafka.TopicMarketSelected}, live.HandleEnvelope},
		{cfg.KafkaConsumerGroupProcess + "-ai", []string{kafka.TopicNewsMarketMatched}, live.HandleEnvelope},
		{cfg.KafkaConsumerGroupProcess + "-probability", []string{kafka.TopicAISignalGenerated}, live.HandleEnvelope},
		{cfg.KafkaConsumerGroupProcess + "-risk", []string{kafka.TopicProbabilityCandidate}, live.HandleEnvelope},
		{cfg.KafkaConsumerGroupProcess + "-execution", []string{kafka.TopicRiskApproved}, live.HandleEnvelope},
		{cfg.KafkaConsumerGroupProcess + "-position", []string{kafka.TopicMarketPriceUpdated, kafka.TopicMarketOrderBookUpdated, kafka.TopicPositionExitCandidate}, live.HandleEnvelope},
		{cfg.KafkaConsumerGroupProcess + "-dashboard", kafka.AllBusinessTopics, func(ctx context.Context, envelope kafka.EventEnvelope) error {
			stream.PublishEnvelope(ctx, envelope)
			return nil
		}},
	}
	consumers := make([]*kafka.Consumer, 0, len(definitions))
	for _, definition := range definitions {
		handlers := map[string]kafka.HandlerFunc{}
		for _, topic := range definition.topics {
			handlers[topic] = definition.handler
		}
		consumer, err := kafka.NewConsumer(kafkaCfg, definition.group, definition.topics, handlers, producer, log)
		if err != nil {
			log.Error("kafka_consumer_failed", "group", definition.group, "error", err)
			continue
		}
		consumers = append(consumers, consumer)
		go func(group string, c *kafka.Consumer) {
			if err := c.Start(ctx); err != nil && ctx.Err() == nil {
				log.Error("kafka_consumer_stopped", "group", group, "error", err)
			}
		}(definition.group, consumer)
	}
	return consumers
}

func closeConsumers(consumers []*kafka.Consumer) {
	for _, consumer := range consumers {
		_ = consumer.Close()
	}
}
