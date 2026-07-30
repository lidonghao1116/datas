package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/basewatch/base-analytics/internal/config"
	"github.com/basewatch/base-analytics/internal/devanalytics"
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

	store, err := clickhousestore.OpenEventStore(
		ctx,
		cfg.ClickHouseAddr,
		cfg.ClickHouseDatabase,
		cfg.ClickHouseUsername,
		cfg.ClickHousePassword,
	)
	if err != nil {
		logger.Error("open Dev analysis store", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	worker, err := devanalytics.NewWorker(
		store,
		devanalytics.NewCalculator(),
		logger,
		devanalytics.WorkerConfig{
			ChainID:         cfg.BaseChainID,
			BatchSize:       int(cfg.DevAnalysisBatchSize),
			EvidenceLimit:   int(cfg.DevAnalysisEvidenceLimit),
			PollInterval:    cfg.DevAnalysisPollInterval,
			RefreshInterval: cfg.DevAnalysisRefreshInterval,
		},
	)
	if err != nil {
		logger.Error("create Dev analysis worker", "error", err)
		os.Exit(1)
	}
	if err := worker.Run(ctx); err != nil {
		logger.Error("Dev analysis worker stopped", "error", err)
		os.Exit(1)
	}
}
