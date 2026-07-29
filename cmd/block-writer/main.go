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
	"github.com/basewatch/base-analytics/internal/writer"
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

	store, err := clickhousestore.OpenRawBlockStore(
		ctx,
		cfg.ClickHouseAddr,
		cfg.ClickHouseDatabase,
		cfg.ClickHouseUsername,
		cfg.ClickHousePassword,
	)
	if err != nil {
		logger.Error("open raw block store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	service, err := writer.NewBlockWriter(
		cfg.RedpandaBrokers,
		cfg.RawBlockTopic,
		cfg.ConsumerGroup,
		store,
		logger,
	)
	if err != nil {
		logger.Error("open block writer", "error", err)
		os.Exit(1)
	}
	defer service.Close()

	if err := service.Run(ctx); err != nil {
		logger.Error("block writer stopped", "error", err)
		os.Exit(1)
	}
}
