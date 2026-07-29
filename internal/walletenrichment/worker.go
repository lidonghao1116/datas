package walletenrichment

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type Worker struct {
	store          Store
	client         Client
	logger         *slog.Logger
	periods        []string
	batchSize      int
	freshness      time.Duration
	activeLookback time.Duration
	pollInterval   time.Duration
	retryBase      time.Duration
}

func NewWorker(
	store Store,
	client Client,
	logger *slog.Logger,
	periods []string,
	batchSize int,
	freshness time.Duration,
	activeLookback time.Duration,
	pollInterval time.Duration,
	retryBase time.Duration,
) (*Worker, error) {
	if store == nil || client == nil || logger == nil {
		return nil, fmt.Errorf("wallet enrichment store, client, and logger are required")
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("wallet enrichment batch size must be positive")
	}
	if freshness <= 0 || activeLookback <= 0 || pollInterval <= 0 || retryBase <= 0 {
		return nil, fmt.Errorf("wallet enrichment durations must be positive")
	}
	cleanPeriods := make([]string, 0, len(periods))
	for _, period := range periods {
		period = strings.TrimSpace(period)
		if period != "7d" && period != "30d" {
			return nil, fmt.Errorf("unsupported GMGN wallet period %q", period)
		}
		cleanPeriods = append(cleanPeriods, period)
	}
	if len(cleanPeriods) == 0 {
		return nil, fmt.Errorf("at least one GMGN wallet period is required")
	}
	return &Worker{
		store:          store,
		client:         client,
		logger:         logger,
		periods:        cleanPeriods,
		batchSize:      batchSize,
		freshness:      freshness,
		activeLookback: activeLookback,
		pollInterval:   pollInterval,
		retryBase:      retryBase,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		processed := 0
		for _, period := range w.periods {
			count, err := w.processPeriod(ctx, period)
			processed += count
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				w.logger.Error("GMGN wallet enrichment cycle failed", "period", period, "error", err)
			}
		}
		if processed > 0 {
			w.logger.Info("GMGN wallet enrichment cycle complete", "processed", processed)
		}
		if wait(ctx, w.pollInterval) != nil {
			return nil
		}
	}
}

func (w *Worker) processPeriod(ctx context.Context, period string) (int, error) {
	candidates, err := w.store.WalletEnrichmentCandidates(
		ctx,
		SourceGMGN,
		period,
		w.freshness,
		w.activeLookback,
		w.batchSize,
	)
	if err != nil {
		return 0, fmt.Errorf("list GMGN wallet candidates: %w", err)
	}
	processed := 0
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			return processed, nil
		}
		attemptedAt := time.Now().UTC()
		stats, fetchErr := w.client.WalletStats(
			ctx,
			"base",
			candidate.WalletAddress,
			period,
		)
		state := SyncState{
			Source:        SourceGMGN,
			ChainID:       candidate.ChainID,
			WalletAddress: candidate.WalletAddress,
			Period:        period,
			AttemptedAt:   attemptedAt,
		}
		if fetchErr != nil {
			state.Status = "failed"
			state.LastError = truncateError(fetchErr.Error(), 1000)
			state.AttemptCount = 1
			state.NextRetryAt = attemptedAt.Add(w.retryBase)
			if err := w.store.RecordWalletEnrichmentSync(ctx, state); err != nil {
				return processed, fmt.Errorf("record failed GMGN sync: %w", err)
			}
			w.logger.Warn(
				"GMGN wallet enrichment failed",
				"wallet_address", candidate.WalletAddress,
				"period", period,
				"error", fetchErr,
			)
			processed++
			continue
		}
		fetchedAt := time.Now().UTC()
		stats.WalletAddress = candidate.WalletAddress
		stats.Period = period
		if err := w.store.InsertWalletEnrichmentSnapshot(ctx, Snapshot{
			ChainID:   candidate.ChainID,
			Stats:     stats,
			Source:    SourceGMGN,
			FetchedAt: fetchedAt,
			ExpiresAt: fetchedAt.Add(w.freshness),
		}); err != nil {
			return processed, fmt.Errorf("insert GMGN wallet snapshot: %w", err)
		}
		state.Status = "success"
		state.NextRetryAt = fetchedAt.Add(w.freshness)
		if err := w.store.RecordWalletEnrichmentSync(ctx, state); err != nil {
			return processed, fmt.Errorf("record successful GMGN sync: %w", err)
		}
		processed++
	}
	return processed, nil
}

func truncateError(message string, limit int) string {
	if len(message) <= limit {
		return message
	}
	return message[:limit]
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
