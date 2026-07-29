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
	"github.com/basewatch/base-analytics/internal/valuation"
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
		logger.Error("open valuation store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	calculator, err := valuation.NewCalculator(
		cfg.ValuationMaxPriceAge,
		cfg.LargeTradeUSD,
	)
	if err != nil {
		logger.Error("create valuation calculator", "error", err)
		os.Exit(1)
	}
	worker, err := valuation.NewWorker(
		store,
		calculator,
		logger,
		int(cfg.ValuationBatchSize),
		cfg.ValuationLookback,
		cfg.ValuationMaxPriceAge,
		cfg.ValuationPollInterval,
	)
	if err != nil {
		logger.Error("create valuation worker", "error", err)
		os.Exit(1)
	}
	if err := worker.Run(ctx); err != nil {
		logger.Error("valuation worker stopped", "error", err)
		os.Exit(1)
	}
}
