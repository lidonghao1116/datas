package devanalytics

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type WorkerConfig struct {
	ChainID         uint64
	BatchSize       int
	EvidenceLimit   int
	PollInterval    time.Duration
	RefreshInterval time.Duration
}

type Worker struct {
	store      Store
	calculator *Calculator
	logger     *slog.Logger
	config     WorkerConfig
}

func NewWorker(
	store Store,
	calculator *Calculator,
	logger *slog.Logger,
	config WorkerConfig,
) (*Worker, error) {
	if store == nil || calculator == nil || logger == nil {
		return nil, fmt.Errorf("Dev analysis store, calculator, and logger are required")
	}
	if config.ChainID == 0 ||
		config.BatchSize <= 0 ||
		config.EvidenceLimit <= 0 ||
		config.PollInterval <= 0 ||
		config.RefreshInterval <= 0 {
		return nil, fmt.Errorf("Dev analysis worker configuration is invalid")
	}
	return &Worker{
		store:      store,
		calculator: calculator,
		logger:     logger,
		config:     config,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		processed, err := w.processBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger.Error("Dev address analysis batch failed", "error", err)
		} else if processed > 0 {
			w.logger.Info("Dev address analysis batch processed", "tokens", processed)
		}
		if processed < w.config.BatchSize {
			if wait(ctx, w.config.PollInterval) != nil {
				return nil
			}
		}
	}
	return nil
}

func (w *Worker) processBatch(ctx context.Context) (int, error) {
	candidates, err := w.store.DevAnalysisCandidates(
		ctx,
		Version,
		w.config.ChainID,
		time.Now().UTC().Add(-w.config.RefreshInterval),
		w.config.BatchSize,
	)
	if err != nil {
		return 0, fmt.Errorf("list Dev analysis candidates: %w", err)
	}
	processed := 0
	for _, candidate := range candidates {
		input, err := w.store.LoadDevAnalysis(ctx, candidate, w.config.EvidenceLimit)
		if err != nil {
			return processed, fmt.Errorf(
				"load Dev analysis %s: %w",
				candidate.TokenAddress,
				err,
			)
		}
		result, err := w.calculator.Calculate(input, time.Now().UTC())
		if err != nil {
			return processed, fmt.Errorf(
				"calculate Dev analysis %s: %w",
				candidate.TokenAddress,
				err,
			)
		}
		if err := w.store.InsertDevAnalysis(ctx, result); err != nil {
			return processed, fmt.Errorf(
				"insert Dev analysis %s: %w",
				candidate.TokenAddress,
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
