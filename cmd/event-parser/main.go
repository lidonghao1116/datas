package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/basewatch/base-analytics/internal/config"
	"github.com/basewatch/base-analytics/internal/eventprocessor"
	"github.com/basewatch/base-analytics/internal/observability"
	"github.com/basewatch/base-analytics/internal/parser/logs"
	"github.com/basewatch/base-analytics/internal/registry"
	clickhousestore "github.com/basewatch/base-analytics/internal/storage/clickhouse"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load configuration", "error", err)
		os.Exit(1)
	}
	logger := observability.NewLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := clickhousestore.OpenEventStore(
		ctx,
		cfg.ClickHouseAddr,
		cfg.ClickHouseDatabase,
		cfg.ClickHouseUsername,
		cfg.ClickHousePassword,
	)
	if err != nil {
		logger.Error("open normalized event store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	registryStore, err := registry.OpenPostgres(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("open pool and token registry", "error", err)
		os.Exit(1)
	}
	defer registryStore.Close()

	contractReader, err := registry.OpenRPCReader(
		ctx,
		cfg.BaseHTTPURL,
		cfg.RPCRequestTimeout,
	)
	if err != nil {
		logger.Error("open registry RPC reader", "error", err)
		os.Exit(1)
	}
	defer contractReader.Close()
	resolver := registry.NewResolver(registryStore, contractReader)

	service, err := eventprocessor.New(
		cfg.RedpandaBrokers,
		cfg.RawBlockTopic,
		cfg.EventParserConsumerGroup,
		logs.NewParser(),
		resolver,
		store,
		logger,
		cfg.RegistryEnrichmentTimeout,
	)
	if err != nil {
		logger.Error("open event parser", "error", err)
		os.Exit(1)
	}
	defer service.Close()

	if err := service.Run(ctx); err != nil {
		logger.Error("event parser stopped", "error", err)
		os.Exit(1)
	}
}
