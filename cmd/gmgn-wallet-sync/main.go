package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/basewatch/base-analytics/internal/config"
	"github.com/basewatch/base-analytics/internal/observability"
	clickhousestore "github.com/basewatch/base-analytics/internal/storage/clickhouse"
	"github.com/basewatch/base-analytics/internal/walletdata/gmgn"
	"github.com/basewatch/base-analytics/internal/walletenrichment"
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
	if cfg.GMGNAPIKey == "" {
		logger.Info("GMGN wallet sync disabled; GMGN_API_KEY is empty")
		<-ctx.Done()
		return
	}

	store, err := clickhousestore.OpenEventStore(
		ctx,
		cfg.ClickHouseAddr,
		cfg.ClickHouseDatabase,
		cfg.ClickHouseUsername,
		cfg.ClickHousePassword,
	)
	if err != nil {
		logger.Error("open GMGN wallet enrichment store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	client, err := gmgn.NewClient(
		cfg.GMGNBaseURL,
		cfg.GMGNAPIKey,
		cfg.GMGNRequestTimeout,
		cfg.GMGNMinRequestInterval,
	)
	if err != nil {
		logger.Error("create GMGN wallet client", "error", err)
		os.Exit(1)
	}
	worker, err := walletenrichment.NewWorker(
		store,
		client,
		logger,
		cfg.GMGNWalletPeriods,
		int(cfg.GMGNWalletSyncBatchSize),
		cfg.GMGNWalletFreshness,
		cfg.GMGNWalletActiveLookback,
		cfg.GMGNWalletSyncInterval,
		cfg.GMGNWalletRetryBase,
	)
	if err != nil {
		logger.Error("create GMGN wallet enrichment worker", "error", err)
		os.Exit(1)
	}
	if err := worker.Run(ctx); err != nil {
		logger.Error("GMGN wallet enrichment worker stopped", "error", err)
		os.Exit(1)
	}
}
