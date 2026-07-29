package backfill

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/basewatch/base-analytics/internal/domain"
)

type Cursor struct {
	ChainID          uint64
	BlockNumber      uint64
	TransactionIndex uint32
	LogIndex         uint32
	EventID          string
}

func CursorFromSwap(swap domain.PoolSwap) Cursor {
	return Cursor{
		ChainID:          swap.ChainID,
		BlockNumber:      swap.BlockNumber,
		TransactionIndex: swap.TransactionIndex,
		LogIndex:         swap.LogIndex,
		EventID:          swap.EventID(),
	}
}

type Store interface {
	PendingSwaps(ctx context.Context, after Cursor, limit int) ([]domain.PoolSwap, error)
	UpsertSwaps(ctx context.Context, swaps []domain.PoolSwap) error
}

type Enricher interface {
	EnrichSwaps(ctx context.Context, swaps []domain.PoolSwap) []error
}

type RegistryWorker struct {
	store         Store
	enricher      Enricher
	logger        *slog.Logger
	batchSize     int
	batchTimeout  time.Duration
	scanInterval  time.Duration
	retryInterval time.Duration
}

func NewRegistryWorker(
	store Store,
	enricher Enricher,
	logger *slog.Logger,
	batchSize int,
	batchTimeout, scanInterval time.Duration,
) (*RegistryWorker, error) {
	if store == nil {
		return nil, fmt.Errorf("backfill store is required")
	}
	if enricher == nil {
		return nil, fmt.Errorf("backfill enricher is required")
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("backfill batch size must be positive")
	}
	if batchTimeout <= 0 {
		return nil, fmt.Errorf("backfill batch timeout must be positive")
	}
	if scanInterval <= 0 {
		return nil, fmt.Errorf("backfill scan interval must be positive")
	}
	return &RegistryWorker{
		store:         store,
		enricher:      enricher,
		logger:        logger,
		batchSize:     batchSize,
		batchTimeout:  batchTimeout,
		scanInterval:  scanInterval,
		retryInterval: time.Second,
	}, nil
}

func (w *RegistryWorker) Run(ctx context.Context) error {
	var cursor Cursor
	for {
		processed, next, err := w.processBatch(ctx, cursor)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger.Error("registry backfill batch failed", "error", err)
			if err := wait(ctx, w.retryInterval); err != nil {
				return nil
			}
			continue
		}
		if processed == 0 {
			cursor = Cursor{}
			w.logger.Info("registry backfill scan complete")
			if err := wait(ctx, w.scanInterval); err != nil {
				return nil
			}
			continue
		}
		cursor = next
	}
}

func (w *RegistryWorker) processBatch(
	ctx context.Context,
	after Cursor,
) (int, Cursor, error) {
	swaps, err := w.store.PendingSwaps(ctx, after, w.batchSize)
	if err != nil {
		return 0, after, fmt.Errorf("list pending swaps: %w", err)
	}
	if len(swaps) == 0 {
		return 0, after, nil
	}
	next := CursorFromSwap(swaps[len(swaps)-1])

	enrichmentCtx, cancel := context.WithTimeout(ctx, w.batchTimeout)
	enrichmentErrors := w.enricher.EnrichSwaps(enrichmentCtx, swaps)
	cancel()
	if len(enrichmentErrors) > 0 {
		w.logger.Warn(
			"registry backfill enrichment incomplete",
			"error_count", len(enrichmentErrors),
			"first_error", enrichmentErrors[0],
		)
	}

	now := time.Now().UTC()
	enriched := make([]domain.PoolSwap, 0, len(swaps))
	for _, swap := range swaps {
		if swap.MetadataStatus == "" || swap.MetadataStatus == "unresolved" {
			continue
		}
		swap.ObservedAt = now
		enriched = append(enriched, swap)
	}
	if err := w.store.UpsertSwaps(ctx, enriched); err != nil {
		return 0, after, fmt.Errorf("upsert enriched swaps: %w", err)
	}
	w.logger.Info(
		"registry backfill batch processed",
		"scanned", len(swaps),
		"upserted", len(enriched),
		"last_block", next.BlockNumber,
	)
	return len(swaps), next, nil
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
