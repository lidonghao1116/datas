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
	ReorgMaxDepth                uint64
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
	AlertSmartScoreVersion       string
	AlertSmartScoreMin           string
	AlertSmartConfidenceMin      string
	AlertSmartTradeMinUSD        string
	AlertWebhookURL              string
	AlertWebhookSecret           string
	AlertDeliveryBatchSize       uint64
	AlertDeliveryLease           time.Duration
	AlertDeliveryPollInterval    time.Duration
	AlertDeliveryTimeout         time.Duration
	AlertDeliveryMaxAttempts     uint64
	AlertDeliveryRetryBase       time.Duration
	AlertDeliveryRetryMax        time.Duration
	APIListenAddress             string
	APIAlertPollInterval         time.Duration
	APIAllowedOrigins            []string
	WalletProfileBatchSize       uint64
	WalletProfilePollInterval    time.Duration
	GMGNBaseURL                  string
	GMGNAPIKey                   string
	GMGNRequestTimeout           time.Duration
	GMGNMinRequestInterval       time.Duration
	GMGNWalletPeriods            []string
	GMGNWalletSyncBatchSize      uint64
	GMGNWalletFreshness          time.Duration
	GMGNWalletActiveLookback     time.Duration
	GMGNWalletSyncInterval       time.Duration
	GMGNWalletRetryBase          time.Duration
	WalletAnalyticsBatchSize     uint64
	WalletAnalyticsPollInterval  time.Duration
	WalletAnalyticsMaxPriceAge   time.Duration
	FlashblocksHTTPURL           string
	FlashblocksWSSURL            string
	FlashblocksReconciliationTTL time.Duration
	FlashblocksReconcileBatch    uint64
	FlashblocksReconcileInterval time.Duration
	FlashblocksReconnectDelay    time.Duration
	FlashblocksRequestTimeout    time.Duration
	FlashblocksFallbackPoll      time.Duration
	ArchiveRPCURL                string
	TraceStartBlock              uint64
	TraceBatchSize               uint64
	TracePollInterval            time.Duration
	TraceRequestTimeout          time.Duration
	TraceTracerTimeout           time.Duration
	TraceMinRequestInterval      time.Duration
	TraceMaxAttempts             uint64
	TraceRetryBase               time.Duration
	TraceRetryMax                time.Duration
	DevAnalysisBatchSize         uint64
	DevAnalysisEvidenceLimit     uint64
	DevAnalysisPollInterval      time.Duration
	DevAnalysisRefreshInterval   time.Duration
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
	cfg.ReorgMaxDepth, err = uintEnv("REORG_MAX_DEPTH", 128)
	if err != nil {
		return Config{}, err
	}
	if cfg.ReorgMaxDepth == 0 {
		return Config{}, fmt.Errorf("REORG_MAX_DEPTH must be positive")
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
	cfg.AlertSmartScoreVersion = env("ALERT_SMART_SCORE_VERSION", "smart-v1")
	cfg.AlertSmartScoreMin = env("ALERT_SMART_SCORE_MIN", "65")
	cfg.AlertSmartConfidenceMin = env("ALERT_SMART_CONFIDENCE_MIN", "0.6")
	cfg.AlertSmartTradeMinUSD = env("ALERT_SMART_TRADE_MIN_USD", "1000")
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
	cfg.APIListenAddress = env("API_LISTEN_ADDRESS", ":8080")
	cfg.APIAlertPollInterval, err = durationEnv("API_ALERT_POLL_INTERVAL", time.Second)
	if err != nil {
		return Config{}, err
	}
	if cfg.APIAlertPollInterval <= 0 {
		return Config{}, fmt.Errorf("API_ALERT_POLL_INTERVAL must be positive")
	}
	cfg.APIAllowedOrigins = splitEnv(
		"API_ALLOWED_ORIGINS",
		"http://localhost:3000,http://localhost:5173",
	)
	cfg.WalletProfileBatchSize, err = uintEnv("WALLET_PROFILE_BATCH_SIZE", 1000)
	if err != nil {
		return Config{}, err
	}
	if cfg.WalletProfileBatchSize == 0 {
		return Config{}, fmt.Errorf("WALLET_PROFILE_BATCH_SIZE must be positive")
	}
	cfg.WalletProfilePollInterval, err = durationEnv(
		"WALLET_PROFILE_POLL_INTERVAL",
		2*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	if cfg.WalletProfilePollInterval <= 0 {
		return Config{}, fmt.Errorf("WALLET_PROFILE_POLL_INTERVAL must be positive")
	}
	cfg.GMGNBaseURL = env("GMGN_BASE_URL", "https://openapi.gmgn.ai")
	cfg.GMGNAPIKey = strings.TrimSpace(os.Getenv("GMGN_API_KEY"))
	cfg.GMGNRequestTimeout, err = durationEnv("GMGN_REQUEST_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.GMGNMinRequestInterval, err = durationEnv(
		"GMGN_MIN_REQUEST_INTERVAL",
		5*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.GMGNWalletPeriods = splitEnv("GMGN_WALLET_PERIODS", "7d,30d")
	cfg.GMGNWalletSyncBatchSize, err = uintEnv("GMGN_WALLET_SYNC_BATCH_SIZE", 5)
	if err != nil {
		return Config{}, err
	}
	if cfg.GMGNWalletSyncBatchSize == 0 {
		return Config{}, fmt.Errorf("GMGN_WALLET_SYNC_BATCH_SIZE must be positive")
	}
	cfg.GMGNWalletFreshness, err = durationEnv("GMGN_WALLET_FRESHNESS", time.Hour)
	if err != nil {
		return Config{}, err
	}
	cfg.GMGNWalletActiveLookback, err = durationEnv(
		"GMGN_WALLET_ACTIVE_LOOKBACK",
		24*time.Hour,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.GMGNWalletSyncInterval, err = durationEnv(
		"GMGN_WALLET_SYNC_INTERVAL",
		5*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.GMGNWalletRetryBase, err = durationEnv("GMGN_WALLET_RETRY_BASE", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	if cfg.GMGNRequestTimeout <= 0 ||
		cfg.GMGNMinRequestInterval < 0 ||
		cfg.GMGNWalletFreshness <= 0 ||
		cfg.GMGNWalletActiveLookback <= 0 ||
		cfg.GMGNWalletSyncInterval <= 0 ||
		cfg.GMGNWalletRetryBase <= 0 {
		return Config{}, fmt.Errorf("GMGN durations must be positive")
	}
	cfg.WalletAnalyticsBatchSize, err = uintEnv("WALLET_ANALYTICS_BATCH_SIZE", 20)
	if err != nil {
		return Config{}, err
	}
	if cfg.WalletAnalyticsBatchSize == 0 {
		return Config{}, fmt.Errorf("WALLET_ANALYTICS_BATCH_SIZE must be positive")
	}
	cfg.WalletAnalyticsPollInterval, err = durationEnv(
		"WALLET_ANALYTICS_POLL_INTERVAL",
		5*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.WalletAnalyticsMaxPriceAge, err = durationEnv(
		"WALLET_ANALYTICS_MAX_PRICE_AGE",
		2*time.Hour,
	)
	if err != nil {
		return Config{}, err
	}
	if cfg.WalletAnalyticsPollInterval <= 0 || cfg.WalletAnalyticsMaxPriceAge <= 0 {
		return Config{}, fmt.Errorf("wallet analytics durations must be positive")
	}
	cfg.FlashblocksHTTPURL = env(
		"FLASHBLOCKS_HTTP_URL",
		"https://mainnet-preconf.base.org",
	)
	cfg.FlashblocksWSSURL = env(
		"FLASHBLOCKS_WSS_URL",
		"wss://mainnet-preconf.base.org",
	)
	cfg.FlashblocksReconciliationTTL, err = durationEnv(
		"FLASHBLOCKS_RECONCILIATION_TTL",
		30*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.FlashblocksReconcileBatch, err = uintEnv("FLASHBLOCKS_RECONCILE_BATCH", 100)
	if err != nil {
		return Config{}, err
	}
	cfg.FlashblocksReconcileInterval, err = durationEnv(
		"FLASHBLOCKS_RECONCILE_INTERVAL",
		time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.FlashblocksReconnectDelay, err = durationEnv(
		"FLASHBLOCKS_RECONNECT_DELAY",
		30*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.FlashblocksRequestTimeout, err = durationEnv(
		"FLASHBLOCKS_REQUEST_TIMEOUT",
		2*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.FlashblocksFallbackPoll, err = durationEnv(
		"FLASHBLOCKS_FALLBACK_POLL_INTERVAL",
		2*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	if cfg.FlashblocksHTTPURL == "" || cfg.FlashblocksWSSURL == "" ||
		cfg.FlashblocksReconciliationTTL <= 0 ||
		cfg.FlashblocksReconcileBatch == 0 ||
		cfg.FlashblocksReconcileInterval <= 0 ||
		cfg.FlashblocksReconnectDelay <= 0 ||
		cfg.FlashblocksRequestTimeout <= 0 ||
		cfg.FlashblocksFallbackPoll <= 0 {
		return Config{}, fmt.Errorf("Flashblocks configuration is invalid")
	}
	cfg.ArchiveRPCURL = env("ARCHIVE_RPC_URL", cfg.BaseHTTPURL)
	cfg.TraceStartBlock, err = uintEnv("TRACE_START_BLOCK", cfg.StartBlock)
	if err != nil {
		return Config{}, err
	}
	cfg.TraceBatchSize, err = uintEnv("TRACE_BATCH_SIZE", 5)
	if err != nil {
		return Config{}, err
	}
	cfg.TracePollInterval, err = durationEnv("TRACE_POLL_INTERVAL", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.TraceRequestTimeout, err = durationEnv("TRACE_REQUEST_TIMEOUT", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.TraceTracerTimeout, err = durationEnv("TRACE_TRACER_TIMEOUT", 20*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.TraceMinRequestInterval, err = durationEnv(
		"TRACE_MIN_REQUEST_INTERVAL",
		time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.TraceMaxAttempts, err = uintEnv("TRACE_MAX_ATTEMPTS", 5)
	if err != nil {
		return Config{}, err
	}
	cfg.TraceRetryBase, err = durationEnv("TRACE_RETRY_BASE", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.TraceRetryMax, err = durationEnv("TRACE_RETRY_MAX", 30*time.Minute)
	if err != nil {
		return Config{}, err
	}
	if cfg.ArchiveRPCURL == "" ||
		cfg.TraceBatchSize == 0 ||
		cfg.TracePollInterval <= 0 ||
		cfg.TraceRequestTimeout <= 0 ||
		cfg.TraceTracerTimeout <= 0 ||
		cfg.TraceMinRequestInterval < 0 ||
		cfg.TraceMaxAttempts == 0 ||
		cfg.TraceRetryBase <= 0 ||
		cfg.TraceRetryMax < cfg.TraceRetryBase {
		return Config{}, fmt.Errorf("transaction trace configuration is invalid")
	}
	cfg.DevAnalysisBatchSize, err = uintEnv("DEV_ANALYSIS_BATCH_SIZE", 10)
	if err != nil {
		return Config{}, err
	}
	cfg.DevAnalysisEvidenceLimit, err = uintEnv("DEV_ANALYSIS_EVIDENCE_LIMIT", 50)
	if err != nil {
		return Config{}, err
	}
	cfg.DevAnalysisPollInterval, err = durationEnv(
		"DEV_ANALYSIS_POLL_INTERVAL",
		30*time.Second,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.DevAnalysisRefreshInterval, err = durationEnv(
		"DEV_ANALYSIS_REFRESH_INTERVAL",
		6*time.Hour,
	)
	if err != nil {
		return Config{}, err
	}
	if cfg.DevAnalysisBatchSize == 0 ||
		cfg.DevAnalysisEvidenceLimit == 0 ||
		cfg.DevAnalysisPollInterval <= 0 ||
		cfg.DevAnalysisRefreshInterval <= 0 {
		return Config{}, fmt.Errorf("Dev analysis configuration is invalid")
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
