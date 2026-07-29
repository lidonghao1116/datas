package marketsync

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/basewatch/base-analytics/internal/marketdata"
)

type TokenSource interface {
	ListMarketTokens(
		ctx context.Context,
		chainID uint64,
		afterAddress string,
		limit int,
	) ([]marketdata.Token, error)
}

type SnapshotStore interface {
	InsertMarketSnapshots(ctx context.Context, snapshots []marketdata.MarketSnapshot) error
	InsertRiskSnapshots(ctx context.Context, snapshots []marketdata.RiskSnapshot) error
}

type Worker struct {
	source       TokenSource
	provider     marketdata.Provider
	store        SnapshotStore
	logger       *slog.Logger
	chainID      uint64
	marketBatch  int
	riskBatch    int
	syncInterval time.Duration
	riskAfter    string
}

func New(
	source TokenSource,
	provider marketdata.Provider,
	store SnapshotStore,
	logger *slog.Logger,
	chainID uint64,
	marketBatch, riskBatch int,
	syncInterval time.Duration,
) (*Worker, error) {
	if source == nil || provider == nil || store == nil {
		return nil, fmt.Errorf("market sync source, provider, and store are required")
	}
	if marketBatch <= 0 || marketBatch > 200 {
		return nil, fmt.Errorf("market sync batch size must be between 1 and 200")
	}
	if riskBatch < 0 {
		return nil, fmt.Errorf("market risk batch size cannot be negative")
	}
	if syncInterval <= 0 {
		return nil, fmt.Errorf("market sync interval must be positive")
	}
	return &Worker{
		source:       source,
		provider:     provider,
		store:        store,
		logger:       logger,
		chainID:      chainID,
		marketBatch:  marketBatch,
		riskBatch:    riskBatch,
		syncInterval: syncInterval,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		if err := w.sync(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger.Error("market data sync failed", "error", err)
		}
		if err := wait(ctx, w.syncInterval); err != nil {
			return nil
		}
	}
}

func (w *Worker) sync(ctx context.Context) error {
	marketCount, err := w.syncMarkets(ctx)
	if err != nil {
		return err
	}
	riskCount, riskErrors, err := w.syncRisks(ctx)
	if err != nil {
		return err
	}
	w.logger.Info(
		"market data sync complete",
		"market_snapshots", marketCount,
		"risk_snapshots", riskCount,
		"risk_errors", riskErrors,
	)
	return nil
}

func (w *Worker) syncMarkets(ctx context.Context) (int, error) {
	total := 0
	after := ""
	for {
		tokens, err := w.source.ListMarketTokens(ctx, w.chainID, after, w.marketBatch)
		if err != nil {
			return total, fmt.Errorf("list market tokens: %w", err)
		}
		if len(tokens) == 0 {
			return total, nil
		}
		snapshots, err := w.provider.MarketSnapshots(ctx, tokens)
		if err != nil {
			return total, fmt.Errorf("fetch market snapshots: %w", err)
		}
		if err := w.store.InsertMarketSnapshots(ctx, snapshots); err != nil {
			return total, fmt.Errorf("store market snapshots: %w", err)
		}
		total += len(snapshots)
		after = tokens[len(tokens)-1].Address
		if len(tokens) < w.marketBatch {
			return total, nil
		}
	}
}

func (w *Worker) syncRisks(ctx context.Context) (int, int, error) {
	if w.riskBatch == 0 {
		return 0, 0, nil
	}
	tokens, err := w.source.ListMarketTokens(ctx, w.chainID, w.riskAfter, w.riskBatch)
	if err != nil {
		return 0, 0, fmt.Errorf("list risk tokens: %w", err)
	}
	if len(tokens) == 0 && w.riskAfter != "" {
		w.riskAfter = ""
		tokens, err = w.source.ListMarketTokens(ctx, w.chainID, "", w.riskBatch)
		if err != nil {
			return 0, 0, fmt.Errorf("restart risk token scan: %w", err)
		}
	}
	if len(tokens) == 0 {
		return 0, 0, nil
	}
	snapshots := make([]marketdata.RiskSnapshot, 0, len(tokens))
	errorCount := 0
	for _, token := range tokens {
		snapshot, err := w.provider.RiskSnapshot(ctx, token)
		if err != nil {
			errorCount++
			continue
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := w.store.InsertRiskSnapshots(ctx, snapshots); err != nil {
		return 0, errorCount, fmt.Errorf("store risk snapshots: %w", err)
	}
	w.riskAfter = tokens[len(tokens)-1].Address
	return len(snapshots), errorCount, nil
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
