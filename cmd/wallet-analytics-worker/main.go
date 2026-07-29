package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/basewatch/base-analytics/internal/config"
	"github.com/basewatch/base-analytics/internal/observability"
	clickhousestore "github.com/basewatch/base-analytics/internal/storage/clickhouse"
	"github.com/basewatch/base-analytics/internal/walletanalytics"
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
		logger.Error("open wallet analytics store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	calculator, err := walletanalytics.NewCalculator(
		strings.Split(cfg.AlertQuoteSymbols, ","),
		cfg.WalletAnalyticsMaxPriceAge,
	)
	if err != nil {
		logger.Error("create wallet analytics calculator", "error", err)
		os.Exit(1)
	}
	worker, err := walletanalytics.NewWorker(
		store,
		calculator,
		logger,
		int(cfg.WalletAnalyticsBatchSize),
		cfg.WalletAnalyticsPollInterval,
	)
	if err != nil {
		logger.Error("create wallet analytics worker", "error", err)
		os.Exit(1)
	}
	if err := worker.Run(ctx); err != nil {
		logger.Error("wallet analytics worker stopped", "error", err)
		os.Exit(1)
	}
}
