package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	AppEnv   string
	AppPort  string
	LogLevel string

	DatabaseURL   string
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	EventBus                  string
	NatsURL                   string
	KafkaEnabled              bool
	KafkaBrokers              []string
	KafkaClientID             string
	KafkaConsumerGroupFetch   string
	KafkaConsumerGroupProcess string
	KafkaAutoCreateTopics     bool

	DashboardSSEEnabled     bool
	DashboardSSEHeartbeat   time.Duration
	DashboardMarketThrottle time.Duration

	PolymarketGammaBaseURL string
	PolymarketCLOBBaseURL  string
	PolymarketWSMarketURL  string
	PolymarketWSUserURL    string

	NewsGDELTBaseURL         string
	NewsPollInterval         time.Duration
	NewsRSSFeeds             []string
	NewsGDELTQuery           string
	NewsMarketMatchThreshold float64
	MaxAIMatchesPerArticle   int

	AIProvider           string
	AIBaseURL            string
	AIAPIKey             string
	AIModel              string
	AITemperature        float64
	AITimeout            time.Duration
	AIRateLimitPerMinute int

	MarketMinVolumeUSD      float64
	MarketMaxSpread         float64
	MarketMinLiquidityUSD   float64
	MarketMinHoursToExpiry  float64
	MaxSubscribedMarkets    int
	EdgeThreshold           float64
	MinSignalConfidence     float64
	MaxPositionPerMarketPct float64
	MaxCategoryExposurePct  float64
	MaxDailyLossPct         float64
	MaxTotalOpenPositions   int
	MaxOrderbookImpactPct   float64
	KellyFraction           float64

	PaperStartingBalanceUSD float64
	ExecutionMode           string
	EnableRealTrading       bool
	RealTradingConfirmation string
	PolyAPIKey              string
	PolyAPISecret           string
	PolyAPIPassphrase       string
	PolyPrivateKey          string
	PolyFunderAddress       string

	TakeProfitPct                    float64
	StopLossPct                      float64
	ExitBeforeExpiryHours            float64
	LiveLoopInterval                 time.Duration
	LiveAutoStart                    bool
	LiveRunOncePublishPipelineEvents bool
}

func Load() Config {
	_ = godotenv.Load()
	return Config{
		AppEnv:   get("APP_ENV", "local"),
		AppPort:  get("APP_PORT", "8080"),
		LogLevel: get("LOG_LEVEL", "debug"),

		DatabaseURL:   get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/polymarket_bot?sslmode=disable"),
		RedisAddr:     get("REDIS_ADDR", "localhost:6379"),
		RedisPassword: get("REDIS_PASSWORD", ""),
		RedisDB:       getInt("REDIS_DB", 0),

		EventBus:                  get("EVENT_BUS", "kafka"),
		NatsURL:                   get("NATS_URL", "nats://localhost:4222"),
		KafkaEnabled:              getBool("KAFKA_ENABLED", true),
		KafkaBrokers:              splitCSV(get("KAFKA_BROKERS", "localhost:29092")),
		KafkaClientID:             get("KAFKA_CLIENT_ID", "ai-trade-local"),
		KafkaConsumerGroupFetch:   get("KAFKA_CONSUMER_GROUP_FETCH", "polymarket-fetch-data-service-local"),
		KafkaConsumerGroupProcess: get("KAFKA_CONSUMER_GROUP_PROCESS", "polymarket-process-service-local"),
		KafkaAutoCreateTopics:     getBool("KAFKA_AUTO_CREATE_TOPICS", true),

		DashboardSSEEnabled:     getBool("DASHBOARD_SSE_ENABLED", true),
		DashboardSSEHeartbeat:   time.Duration(getInt("DASHBOARD_SSE_HEARTBEAT_SECONDS", 15)) * time.Second,
		DashboardMarketThrottle: time.Duration(getInt("DASHBOARD_MARKET_THROTTLE_MS", 1000)) * time.Millisecond,

		PolymarketGammaBaseURL: strings.TrimRight(get("POLYMARKET_GAMMA_BASE_URL", "https://gamma-api.polymarket.com"), "/"),
		PolymarketCLOBBaseURL:  strings.TrimRight(get("POLYMARKET_CLOB_BASE_URL", "https://clob.polymarket.com"), "/"),
		PolymarketWSMarketURL:  get("POLYMARKET_WS_MARKET_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
		PolymarketWSUserURL:    get("POLYMARKET_WS_USER_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/user"),

		NewsGDELTBaseURL:         get("NEWS_GDELT_BASE_URL", "https://api.gdeltproject.org/api/v2/doc/doc"),
		NewsPollInterval:         time.Duration(getInt("NEWS_POLL_INTERVAL_SECONDS", 60)) * time.Second,
		NewsRSSFeeds:             splitCSV(get("NEWS_RSS_FEEDS", "https://www.coindesk.com/arc/outboundfeeds/rss/,https://cointelegraph.com/rss,https://www.sec.gov/news/pressreleases.rss")),
		NewsGDELTQuery:           get("NEWS_GDELT_QUERY", "(bitcoin OR ethereum OR crypto OR SEC OR fed OR election OR sports) sourcelang:English"),
		NewsMarketMatchThreshold: getFloat("NEWS_MARKET_MATCH_THRESHOLD", 0.70),
		MaxAIMatchesPerArticle:   getInt("MAX_AI_MATCHES_PER_ARTICLE", 3),

		AIProvider:           get("AI_PROVIDER", "openai_compatible"),
		AIBaseURL:            strings.TrimRight(get("AI_BASE_URL", "https://api.z.ai/api/paas/v4"), "/"),
		AIAPIKey:             get("AI_API_KEY", ""),
		AIModel:              get("AI_MODEL", "glm-5.1"),
		AITemperature:        getFloat("AI_TEMPERATURE", 0.1),
		AITimeout:            time.Duration(getInt("AI_TIMEOUT_SECONDS", 60)) * time.Second,
		AIRateLimitPerMinute: getInt("AI_RATE_LIMIT_PER_MINUTE", 30),

		MarketMinVolumeUSD:      getFloat("MARKET_MIN_VOLUME_USD", 50000),
		MarketMaxSpread:         getFloat("MARKET_MAX_SPREAD", 0.05),
		MarketMinLiquidityUSD:   getFloat("MARKET_MIN_LIQUIDITY_USD", 10000),
		MarketMinHoursToExpiry:  getFloat("MARKET_MIN_HOURS_TO_EXPIRY", 12),
		MaxSubscribedMarkets:    getInt("MAX_SUBSCRIBED_MARKETS", 50),
		EdgeThreshold:           getFloat("EDGE_THRESHOLD", 0.05),
		MinSignalConfidence:     getFloat("MIN_SIGNAL_CONFIDENCE", 0.65),
		MaxPositionPerMarketPct: getFloat("MAX_POSITION_PER_MARKET_PCT", 0.05),
		MaxCategoryExposurePct:  getFloat("MAX_CATEGORY_EXPOSURE_PCT", 0.20),
		MaxDailyLossPct:         getFloat("MAX_DAILY_LOSS_PCT", 0.03),
		MaxTotalOpenPositions:   getInt("MAX_TOTAL_OPEN_POSITIONS", 10),
		MaxOrderbookImpactPct:   getFloat("MAX_ORDERBOOK_IMPACT_PCT", 0.02),
		KellyFraction:           getFloat("KELLY_FRACTION", 0.25),

		PaperStartingBalanceUSD: getFloat("PAPER_STARTING_BALANCE_USD", 10000),
		ExecutionMode:           get("EXECUTION_MODE", "paper"),
		EnableRealTrading:       getBool("ENABLE_REAL_TRADING", false),
		RealTradingConfirmation: get("REAL_TRADING_CONFIRMATION", ""),
		PolyAPIKey:              get("POLY_API_KEY", ""),
		PolyAPISecret:           get("POLY_API_SECRET", ""),
		PolyAPIPassphrase:       get("POLY_API_PASSPHRASE", ""),
		PolyPrivateKey:          get("POLY_PRIVATE_KEY", ""),
		PolyFunderAddress:       get("POLY_FUNDER_ADDRESS", ""),

		TakeProfitPct:                    getFloat("TAKE_PROFIT_PCT", 0.20),
		StopLossPct:                      getFloat("STOP_LOSS_PCT", 0.10),
		ExitBeforeExpiryHours:            getFloat("EXIT_BEFORE_EXPIRY_HOURS", 6),
		LiveLoopInterval:                 time.Duration(getInt("LIVE_LOOP_INTERVAL_SECONDS", 60)) * time.Second,
		LiveAutoStart:                    getBool("LIVE_AUTO_START", false),
		LiveRunOncePublishPipelineEvents: getBool("LIVE_RUN_ONCE_PUBLISH_PIPELINE_EVENTS", false),
	}
}

func (c Config) RedactedFields() map[string]any {
	return map[string]any{
		"app_env": c.AppEnv, "app_port": c.AppPort, "execution_mode": c.ExecutionMode,
		"ai_provider": c.AIProvider, "ai_model": c.AIModel, "ai_key_configured": c.AIAPIKey != "",
		"real_trading_enabled": c.EnableRealTrading,
		"kafka_enabled":        c.KafkaEnabled, "kafka_brokers": c.KafkaBrokers,
	}
}

func get(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getInt(key string, fallback int) int {
	v, err := strconv.Atoi(get(key, ""))
	if err != nil {
		return fallback
	}
	return v
}

func getFloat(key string, fallback float64) float64 {
	v, err := strconv.ParseFloat(get(key, ""), 64)
	if err != nil {
		return fallback
	}
	return v
}

func getBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(strings.ToLower(get(key, "")))
	if raw == "" {
		return fallback
	}
	return raw == "1" || raw == "true" || raw == "yes"
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
