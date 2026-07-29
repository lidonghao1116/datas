package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/basewatch/base-analytics/internal/alerting"
	"github.com/basewatch/base-analytics/internal/config"
	"github.com/basewatch/base-analytics/internal/observability"
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

	source, err := clickhousestore.OpenEventStore(
		ctx,
		cfg.ClickHouseAddr,
		cfg.ClickHouseDatabase,
		cfg.ClickHouseUsername,
		cfg.ClickHousePassword,
	)
	if err != nil {
		logger.Error("open alert candidate store", "error", err)
		os.Exit(1)
	}
	defer source.Close()

	outbox, err := alerting.OpenPostgresOutbox(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("open alert outbox", "error", err)
		os.Exit(1)
	}
	defer outbox.Close()

	engine, err := alerting.NewEngine(
		source,
		outbox,
		logger,
		alerting.SortedQuoteSymbols(cfg.AlertQuoteSymbols),
		cfg.AlertCriticalUSD,
		cfg.AlertLookback,
		int(cfg.AlertBatchSize),
		cfg.AlertPollInterval,
	)
	if err != nil {
		logger.Error("create alert engine", "error", err)
		os.Exit(1)
	}
	if err := engine.Run(ctx); err != nil {
		logger.Error("alert engine stopped", "error", err)
		os.Exit(1)
	}
}
