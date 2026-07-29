package valuation

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type Worker struct {
	store        Store
	calculator   *Calculator
	logger       *slog.Logger
	batchSize    int
	lookback     time.Duration
	maxPriceAge  time.Duration
	pollInterval time.Duration
}

func NewWorker(
	store Store,
	calculator *Calculator,
	logger *slog.Logger,
	batchSize int,
	lookback, maxPriceAge, pollInterval time.Duration,
) (*Worker, error) {
	if store == nil || calculator == nil {
		return nil, fmt.Errorf("valuation store and calculator are required")
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("valuation batch size must be positive")
	}
	if lookback <= 0 || maxPriceAge <= 0 || pollInterval <= 0 {
		return nil, fmt.Errorf("valuation durations must be positive")
	}
	return &Worker{
		store:        store,
		calculator:   calculator,
		logger:       logger,
		batchSize:    batchSize,
		lookback:     lookback,
		maxPriceAge:  maxPriceAge,
		pollInterval: pollInterval,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		scanned, valued, skipped, err := w.processBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger.Error("swap valuation batch failed", "error", err)
			if err := wait(ctx, w.pollInterval); err != nil {
				return nil
			}
			continue
		}
		if scanned > 0 {
			w.logger.Info(
				"swap valuation batch processed",
				"scanned", scanned,
				"valued", valued,
				"skipped", skipped,
			)
		}
		if scanned < w.batchSize {
			if err := wait(ctx, w.pollInterval); err != nil {
				return nil
			}
		}
	}
}

func (w *Worker) processBatch(ctx context.Context) (int, int, int, error) {
	candidates, err := w.store.ValuationCandidates(
		ctx,
		w.lookback,
		w.maxPriceAge,
		w.batchSize,
	)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("list valuation candidates: %w", err)
	}
	results := make([]Result, 0, len(candidates))
	skipped := 0
	for _, candidate := range candidates {
		result, valued, err := w.calculator.Value(candidate)
		if err != nil || !valued {
			skipped++
			continue
		}
		results = append(results, result)
	}
	if err := w.store.InsertValuations(ctx, results); err != nil {
		return 0, 0, 0, fmt.Errorf("insert swap valuations: %w", err)
	}
	return len(candidates), len(results), skipped, nil
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
