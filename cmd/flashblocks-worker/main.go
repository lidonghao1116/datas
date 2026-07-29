package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/basewatch/base-analytics/internal/alerting"
	"github.com/basewatch/base-analytics/internal/config"
	"github.com/basewatch/base-analytics/internal/flashblocks"
	"github.com/basewatch/base-analytics/internal/observability"
	parserlogs "github.com/basewatch/base-analytics/internal/parser/logs"
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
		logger.Error("open Flashblocks enrichment store", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	outbox, err := alerting.OpenPostgresOutbox(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("open Flashblocks alert outbox", "error", err)
		os.Exit(1)
	}
	defer outbox.Close()
	state, err := flashblocks.OpenPostgresStateStore(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("open Flashblocks state store", "error", err)
		os.Exit(1)
	}
	defer state.Close()
	detector, err := alerting.NewEngine(
		store,
		outbox,
		logger,
		alerting.EngineConfig{
			QuoteSymbols:       alerting.SortedQuoteSymbols(cfg.AlertQuoteSymbols),
			CriticalUSD:        cfg.AlertCriticalUSD,
			SmartScoreVersion:  cfg.AlertSmartScoreVersion,
			SmartScoreMin:      cfg.AlertSmartScoreMin,
			SmartConfidenceMin: cfg.AlertSmartConfidenceMin,
			SmartTradeMinUSD:   cfg.AlertSmartTradeMinUSD,
			Lookback:           cfg.AlertLookback,
			BatchSize:          int(cfg.AlertBatchSize),
			PollInterval:       cfg.AlertPollInterval,
		},
	)
	if err != nil {
		logger.Error("create Flashblocks alert detector", "error", err)
		os.Exit(1)
	}
	calculator, err := valuation.NewCalculator(
		cfg.ValuationMaxPriceAge,
		cfg.LargeTradeUSD,
	)
	if err != nil {
		logger.Error("create Flashblocks valuation calculator", "error", err)
		os.Exit(1)
	}
	client := flashblocks.NewClient(
		cfg.FlashblocksHTTPURL,
		cfg.BaseHTTPURL,
		cfg.FlashblocksWSSURL,
	)
	worker, err := flashblocks.NewWorker(
		client,
		store,
		state,
		detector,
		parserlogs.NewParser(),
		calculator,
		logger,
		flashblocks.WorkerConfig{
			ChainID:              cfg.BaseChainID,
			ScoreVersion:         cfg.AlertSmartScoreVersion,
			ReconciliationTTL:    cfg.FlashblocksReconciliationTTL,
			ReconciliationBatch:  int(cfg.FlashblocksReconcileBatch),
			ReconciliationPoll:   cfg.FlashblocksReconcileInterval,
			ReconnectDelay:       cfg.FlashblocksReconnectDelay,
			RequestTimeout:       cfg.FlashblocksRequestTimeout,
			FallbackPollInterval: cfg.FlashblocksFallbackPoll,
		},
	)
	if err != nil {
		logger.Error("create Flashblocks worker", "error", err)
		os.Exit(1)
	}
	if err := worker.Run(ctx); err != nil {
		logger.Error("Flashblocks worker stopped", "error", err)
		os.Exit(1)
	}
}
