package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/basewatch/base-analytics/internal/config"
	"github.com/basewatch/base-analytics/internal/marketdata/ave"
	"github.com/basewatch/base-analytics/internal/marketsync"
	"github.com/basewatch/base-analytics/internal/observability"
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

	tokenSource, err := registry.OpenPostgres(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("open market token source", "error", err)
		os.Exit(1)
	}
	defer tokenSource.Close()

	snapshotStore, err := clickhousestore.OpenEventStore(
		ctx,
		cfg.ClickHouseAddr,
		cfg.ClickHouseDatabase,
		cfg.ClickHouseUsername,
		cfg.ClickHousePassword,
	)
	if err != nil {
		logger.Error("open market snapshot store", "error", err)
		os.Exit(1)
	}
	defer snapshotStore.Close()

	provider, err := ave.NewClient(
		cfg.AVEBaseURL,
		cfg.AVEAPIKey,
		cfg.AVERequestTimeout,
		cfg.AVEMinRequestInterval,
	)
	if err != nil {
		logger.Error("create AVE provider", "error", err)
		os.Exit(1)
	}
	worker, err := marketsync.New(
		tokenSource,
		provider,
		snapshotStore,
		logger,
		cfg.BaseChainID,
		int(cfg.MarketSyncBatchSize),
		int(cfg.MarketRiskBatchSize),
		cfg.MarketSyncInterval,
	)
	if err != nil {
		logger.Error("create market sync worker", "error", err)
		os.Exit(1)
	}
	if err := worker.Run(ctx); err != nil {
		logger.Error("market sync worker stopped", "error", err)
		os.Exit(1)
	}
}
