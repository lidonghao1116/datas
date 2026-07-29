package walletanalytics

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
	pollInterval time.Duration
}

func NewWorker(
	store Store,
	calculator *Calculator,
	logger *slog.Logger,
	batchSize int,
	pollInterval time.Duration,
) (*Worker, error) {
	if store == nil || calculator == nil || logger == nil {
		return nil, fmt.Errorf("wallet analytics store, calculator, and logger are required")
	}
	if batchSize <= 0 || pollInterval <= 0 {
		return nil, fmt.Errorf("wallet analytics batch size and poll interval must be positive")
	}
	return &Worker{
		store:        store,
		calculator:   calculator,
		logger:       logger,
		batchSize:    batchSize,
		pollInterval: pollInterval,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		processed, err := w.processBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger.Error("wallet analytics batch failed", "error", err)
		} else if processed > 0 {
			w.logger.Info("wallet analytics batch processed", "wallets", processed)
		}
		if processed < w.batchSize {
			if wait(ctx, w.pollInterval) != nil {
				return nil
			}
		}
	}
}

func (w *Worker) processBatch(ctx context.Context) (int, error) {
	candidates, err := w.store.WalletAnalysisCandidates(ctx, Version, w.batchSize)
	if err != nil {
		return 0, fmt.Errorf("list wallet analysis candidates: %w", err)
	}
	processed := 0
	for _, candidate := range candidates {
		input, err := w.store.LoadWalletAnalysis(ctx, candidate)
		if err != nil {
			return processed, fmt.Errorf(
				"load wallet analysis %s: %w",
				candidate.WalletAddress,
				err,
			)
		}
		result, err := w.calculator.Calculate(input, time.Now().UTC())
		if err != nil {
			return processed, fmt.Errorf(
				"calculate wallet analysis %s: %w",
				candidate.WalletAddress,
				err,
			)
		}
		if err := w.store.InsertWalletAnalysis(ctx, result); err != nil {
			return processed, fmt.Errorf(
				"insert wallet analysis %s: %w",
				candidate.WalletAddress,
				err,
			)
		}
		processed++
	}
	return processed, nil
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
