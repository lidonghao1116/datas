package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/basewatch/base-analytics/internal/alerting"
	"github.com/basewatch/base-analytics/internal/config"
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
	if cfg.AlertWebhookURL == "" {
		logger.Info("alert dispatcher disabled; ALERT_WEBHOOK_URL is empty")
		<-ctx.Done()
		return
	}

	outbox, err := alerting.OpenPostgresOutbox(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("open alert delivery outbox", "error", err)
		os.Exit(1)
	}
	defer outbox.Close()

	sender, err := alerting.NewWebhookSender(
		cfg.AlertWebhookURL,
		cfg.AlertWebhookSecret,
		cfg.AlertDeliveryTimeout,
	)
	if err != nil {
		logger.Error("create alert webhook sender", "error", err)
		os.Exit(1)
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	dispatcher, err := alerting.NewDispatcher(
		outbox,
		sender,
		logger,
		fmt.Sprintf("%s-%d", hostname, os.Getpid()),
		int(cfg.AlertDeliveryBatchSize),
		cfg.AlertDeliveryLease,
		cfg.AlertDeliveryPollInterval,
		cfg.AlertDeliveryTimeout,
		int(cfg.AlertDeliveryMaxAttempts),
		cfg.AlertDeliveryRetryBase,
		cfg.AlertDeliveryRetryMax,
	)
	if err != nil {
		logger.Error("create alert dispatcher", "error", err)
		os.Exit(1)
	}
	if err := dispatcher.Run(ctx); err != nil {
		logger.Error("alert dispatcher stopped", "error", err)
		os.Exit(1)
	}
}
