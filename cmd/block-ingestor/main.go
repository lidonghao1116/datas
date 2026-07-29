package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/basewatch/base-analytics/internal/chain/base"
	"github.com/basewatch/base-analytics/internal/checkpoint"
	"github.com/basewatch/base-analytics/internal/config"
	"github.com/basewatch/base-analytics/internal/ingest"
	"github.com/basewatch/base-analytics/internal/messaging"
	"github.com/basewatch/base-analytics/internal/observability"
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

	checkpoints, err := checkpoint.OpenPostgres(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("open checkpoint store", "error", err)
		os.Exit(1)
	}
	defer checkpoints.Close()

	publisher, err := messaging.NewKafkaRawBlockPublisher(cfg.RedpandaBrokers, cfg.RawBlockTopic)
	if err != nil {
		logger.Error("open raw block publisher", "error", err)
		os.Exit(1)
	}
	defer publisher.Close()

	chainClient := base.NewClient(
		cfg.BaseHTTPURL,
		cfg.BaseWSSURL,
		cfg.BaseChainID,
		cfg.RPCRequestTimeout,
	)
	service := ingest.NewBlockIngestor(
		chainClient,
		publisher,
		checkpoints,
		cfg.BaseChainID,
		cfg.StartBlock,
		cfg.RPCReconnectDelay,
		logger,
	)
	if err := service.Run(ctx); err != nil {
		logger.Error("block ingestor stopped", "error", err)
		os.Exit(1)
	}
}
