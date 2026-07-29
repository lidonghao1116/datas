package walletprofile

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type Worker struct {
	store        Store
	logger       *slog.Logger
	batchSize    int
	pollInterval time.Duration
}

func NewWorker(
	store Store,
	logger *slog.Logger,
	batchSize int,
	pollInterval time.Duration,
) (*Worker, error) {
	if store == nil {
		return nil, fmt.Errorf("wallet profile store is required")
	}
	if logger == nil {
		return nil, fmt.Errorf("wallet profile logger is required")
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("wallet profile batch size must be positive")
	}
	if pollInterval <= 0 {
		return nil, fmt.Errorf("wallet profile poll interval must be positive")
	}
	return &Worker{
		store:        store,
		logger:       logger,
		batchSize:    batchSize,
		pollInterval: pollInterval,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		scanned, inserted, skipped, err := w.processBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger.Error("wallet profile batch failed", "error", err)
			if wait(ctx, w.pollInterval) != nil {
				return nil
			}
			continue
		}
		if scanned > 0 {
			w.logger.Info(
				"wallet profile batch processed",
				"scanned", scanned,
				"inserted", inserted,
				"skipped", skipped,
			)
		}
		if scanned < w.batchSize && wait(ctx, w.pollInterval) != nil {
			return nil
		}
	}
}

func (w *Worker) processBatch(ctx context.Context) (int, int, int, error) {
	candidates, err := w.store.WalletActivityCandidates(ctx, w.batchSize)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("list wallet activity candidates: %w", err)
	}
	now := time.Now().UTC()
	activities := make([]Activity, 0, len(candidates))
	for _, candidate := range candidates {
		activity, ok := buildActivity(candidate, now)
		if !ok {
			continue
		}
		activities = append(activities, activity)
	}
	if err := w.store.InsertWalletActivities(ctx, activities); err != nil {
		return 0, 0, 0, fmt.Errorf("insert wallet activities: %w", err)
	}
	return len(candidates), len(activities), len(candidates) - len(activities), nil
}

func buildActivity(candidate Candidate, generatedAt time.Time) (Activity, bool) {
	if candidate.EventID == "" ||
		!common.IsHexAddress(candidate.WalletAddress) ||
		!common.IsHexAddress(candidate.BoughtTokenAddress) ||
		!common.IsHexAddress(candidate.SoldTokenAddress) ||
		candidate.SourceValuedAt.IsZero() {
		return Activity{}, false
	}
	candidate.WalletAddress = normalizeAddress(candidate.WalletAddress)
	candidate.RouterAddress = normalizeOptionalAddress(candidate.RouterAddress)
	candidate.BoughtTokenAddress = normalizeAddress(candidate.BoughtTokenAddress)
	candidate.SoldTokenAddress = normalizeAddress(candidate.SoldTokenAddress)
	return Activity{
		Candidate:         candidate,
		AttributionMethod: AttributionTransactionFrom,
		GeneratedAt:       generatedAt,
	}, true
}

func normalizeAddress(address string) string {
	return strings.ToLower(common.HexToAddress(address).Hex())
}

func normalizeOptionalAddress(address string) string {
	if !common.IsHexAddress(address) {
		return ""
	}
	return normalizeAddress(address)
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
