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
	"github.com/basewatch/base-analytics/internal/traceanalytics"
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
		logger.Error("open transaction trace store", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	client, err := traceanalytics.NewRPCClient(
		cfg.ArchiveRPCURL,
		cfg.TraceTracerTimeout.String(),
	)
	if err != nil {
		logger.Error("create archive RPC client", "error", err)
		os.Exit(1)
	}
	worker, err := traceanalytics.NewWorker(
		store,
		client,
		logger,
		traceanalytics.WorkerConfig{
			ChainID:            cfg.BaseChainID,
			StartBlock:         cfg.TraceStartBlock,
			BatchSize:          int(cfg.TraceBatchSize),
			PollInterval:       cfg.TracePollInterval,
			RequestTimeout:     cfg.TraceRequestTimeout,
			MinRequestInterval: cfg.TraceMinRequestInterval,
			MaxAttempts:        uint32(cfg.TraceMaxAttempts),
			RetryBase:          cfg.TraceRetryBase,
			RetryMax:           cfg.TraceRetryMax,
		},
	)
	if err != nil {
		logger.Error("create transaction trace worker", "error", err)
		os.Exit(1)
	}
	if err := worker.Run(ctx); err != nil {
		logger.Error("transaction trace worker stopped", "error", err)
		os.Exit(1)
	}
}
