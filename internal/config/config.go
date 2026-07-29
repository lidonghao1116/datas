package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	BaseHTTPURL                  string
	BaseWSSURL                   string
	BaseChainID                  uint64
	StartBlock                   uint64
	RPCRequestTimeout            time.Duration
	RPCReconnectDelay            time.Duration
	RegistryEnrichmentTimeout    time.Duration
	RegistryBackfillBatchSize    uint64
	RegistryBackfillBatchTimeout time.Duration
	RegistryBackfillScanInterval time.Duration
	AVEBaseURL                   string
	AVEAPIKey                    string
	AVERequestTimeout            time.Duration
	AVEMinRequestInterval        time.Duration
	MarketSyncBatchSize          uint64
	MarketRiskBatchSize          uint64
	MarketSyncInterval           time.Duration
	ValuationBatchSize           uint64
	ValuationLookback            time.Duration
	ValuationMaxPriceAge         time.Duration
	ValuationPollInterval        time.Duration
	LargeTradeUSD                string
	AlertBatchSize               uint64
	AlertLookback                time.Duration
	AlertPollInterval            time.Duration
	AlertCriticalUSD             string
	AlertQuoteSymbols            string
	AlertWebhookURL              string
	AlertWebhookSecret           string
	AlertDeliveryBatchSize       uint64
	AlertDeliveryLease           time.Duration
	AlertDeliveryPollInterval    time.Duration
	AlertDeliveryTimeout         time.Duration
	AlertDeliveryMaxAttempts     uint64
	AlertDeliveryRetryBase       time.Duration
	AlertDeliveryRetryMax        time.Duration
	RedpandaBrokers              []string
	RawBlockTopic                string
	ConsumerGroup                string
	EventParserConsumerGroup     string
	PostgresDSN                  string
	ClickHouseAddr               string
	ClickHouseDatabase           string
	ClickHouseUsername           string
	ClickHousePassword           string
	LogLevel                     string
}

func Load() (Config, error) {
	var cfg Config
	var err error

	cfg.BaseHTTPURL = env("BASE_HTTP_URL", "https://mainnet.base.org")
	cfg.BaseWSSURL = env("BASE_WSS_URL", "wss://mainnet.base.org")
	cfg.BaseChainID, err = uintEnv("BASE_CHAIN_ID", 8453)
	if err != nil {
		return Config{}, err
	}
	cfg.StartBlock, err = uintEnv("START_BLOCK", 0)
	if err != nil {
		return Config{}, err
	}
	cfg.RPCRequestTimeout, err = durationEnv("RPC_REQUEST_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.RPCReconnectDelay, err = durationEnv("RPC_RECONNECT_DELAY", 3*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.RegistryEnrichmentTimeout, err = durationEnv(
		"REGISTRY_ENRICHMENT_TIMEOUT",
		100*time.Millisecond,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.RegistryBackfillBatchSize, err = uintEnv("REGISTRY_BACKFILL_BATCH_SIZE", 100)
	if err != nil {
		return Config{}, err
	}
	if cfg.RegistryBackfillBatchSize == 0 {
		return Config{}, fmt.Errorf("REGISTRY_BACKFILL_BATCH_SIZE must be positive")
	}
	cfg.RegistryBackfillBatchTimeout, err = durationEnv(
		"REGISTRY_BACKFILL_BATCH_TIMEOUT",
		2*time.Minute,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.RegistryBackfillScanInterval, err = durationEnv(
		"REGISTRY_BACKFILL_SCAN_INTERVAL",
		30*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.AVEBaseURL = env("AVE_BASE_URL", "https://prod.ave-api.com")
	cfg.AVEAPIKey = strings.TrimSpace(os.Getenv("AVE_API_KEY"))
	cfg.AVERequestTimeout, err = durationEnv("AVE_REQUEST_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.AVEMinRequestInterval, err = durationEnv(
		"AVE_MIN_REQUEST_INTERVAL",
		250*time.Millisecond,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.MarketSyncBatchSize, err = uintEnv("MARKET_SYNC_BATCH_SIZE", 200)
	if err != nil {
		return Config{}, err
	}
	cfg.MarketRiskBatchSize, err = uintEnv("MARKET_RISK_BATCH_SIZE", 10)
	if err != nil {
		return Config{}, err
	}
	cfg.MarketSyncInterval, err = durationEnv("MARKET_SYNC_INTERVAL", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	cfg.ValuationBatchSize, err = uintEnv("VALUATION_BATCH_SIZE", 500)
	if err != nil {
		return Config{}, err
	}
	cfg.ValuationLookback, err = durationEnv("VALUATION_LOOKBACK", time.Hour)
	if err != nil {
		return Config{}, err
	}
	cfg.ValuationMaxPriceAge, err = durationEnv("VALUATION_MAX_PRICE_AGE", 10*time.Minute)
	if err != nil {
		return Config{}, err
	}
	cfg.ValuationPollInterval, err = durationEnv("VALUATION_POLL_INTERVAL", 2*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.LargeTradeUSD = env("LARGE_TRADE_USD", "10000")
	cfg.AlertBatchSize, err = uintEnv("ALERT_BATCH_SIZE", 500)
	if err != nil {
		return Config{}, err
	}
	cfg.AlertLookback, err = durationEnv("ALERT_LOOKBACK", time.Hour)
	if err != nil {
		return Config{}, err
	}
	cfg.AlertPollInterval, err = durationEnv("ALERT_POLL_INTERVAL", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.AlertCriticalUSD = env("ALERT_CRITICAL_USD", "50000")
	cfg.AlertQuoteSymbols = env(
		"ALERT_QUOTE_SYMBOLS",
		"WETH,USDC,USDBC,USDT,DAI,EURC,CBBTC",
	)
	cfg.AlertWebhookURL = strings.TrimSpace(os.Getenv("ALERT_WEBHOOK_URL"))
	cfg.AlertWebhookSecret = os.Getenv("ALERT_WEBHOOK_SECRET")
	cfg.AlertDeliveryBatchSize, err = uintEnv("ALERT_DELIVERY_BATCH_SIZE", 20)
	if err != nil {
		return Config{}, err
	}
	cfg.AlertDeliveryLease, err = durationEnv("ALERT_DELIVERY_LEASE", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.AlertDeliveryPollInterval, err = durationEnv(
		"ALERT_DELIVERY_POLL_INTERVAL",
		2*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.AlertDeliveryTimeout, err = durationEnv("ALERT_DELIVERY_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.AlertDeliveryMaxAttempts, err = uintEnv("ALERT_DELIVERY_MAX_ATTEMPTS", 8)
	if err != nil {
		return Config{}, err
	}
	cfg.AlertDeliveryRetryBase, err = durationEnv(
		"ALERT_DELIVERY_RETRY_BASE",
		5*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.AlertDeliveryRetryMax, err = durationEnv(
		"ALERT_DELIVERY_RETRY_MAX",
		15*time.Minute,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.RedpandaBrokers = splitEnv("REDPANDA_BROKERS", "localhost:19092")
	cfg.RawBlockTopic = env("REDPANDA_RAW_BLOCK_TOPIC", "base.block.raw.v1")
	cfg.ConsumerGroup = env("REDPANDA_CONSUMER_GROUP", "base-clickhouse-writer-v1")
	cfg.EventParserConsumerGroup = env(
		"REDPANDA_EVENT_PARSER_CONSUMER_GROUP",
		"base-event-parser-v4",
	)
	cfg.PostgresDSN = env("POSTGRES_DSN", "postgres://base:base@localhost:5432/base?sslmode=disable")
	cfg.ClickHouseAddr = env("CLICKHOUSE_ADDR", "localhost:9000")
	cfg.ClickHouseDatabase = env("CLICKHOUSE_DATABASE", "base")
	cfg.ClickHouseUsername = env("CLICKHOUSE_USERNAME", "default")
	cfg.ClickHousePassword = os.Getenv("CLICKHOUSE_PASSWORD")
	cfg.LogLevel = env("LOG_LEVEL", "info")

	if cfg.BaseHTTPURL == "" || cfg.BaseWSSURL == "" {
		return Config{}, fmt.Errorf("BASE_HTTP_URL and BASE_WSS_URL are required")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func splitEnv(key, fallback string) []string {
	values := strings.Split(env(key, fallback), ",")
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func uintEnv(key string, fallback uint64) (uint64, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an unsigned integer: %w", key, err)
	}
	return parsed, nil
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", key, err)
	}
	return parsed, nil
}
