package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	BaseHTTPURL               string
	BaseWSSURL                string
	BaseChainID               uint64
	StartBlock                uint64
	RPCRequestTimeout         time.Duration
	RPCReconnectDelay         time.Duration
	RegistryEnrichmentTimeout time.Duration
	RedpandaBrokers           []string
	RawBlockTopic             string
	ConsumerGroup             string
	EventParserConsumerGroup  string
	PostgresDSN               string
	ClickHouseAddr            string
	ClickHouseDatabase        string
	ClickHouseUsername        string
	ClickHousePassword        string
	LogLevel                  string
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
		time.Second,
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
