package traceanalytics

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type WorkerConfig struct {
	ChainID            uint64
	StartBlock         uint64
	BatchSize          int
	PollInterval       time.Duration
	RequestTimeout     time.Duration
	MinRequestInterval time.Duration
	MaxAttempts        uint32
	RetryBase          time.Duration
	RetryMax           time.Duration
}

type Worker struct {
	store  Store
	client Client
	logger *slog.Logger
	config WorkerConfig
}

func NewWorker(
	store Store,
	client Client,
	logger *slog.Logger,
	config WorkerConfig,
) (*Worker, error) {
	if store == nil || client == nil || logger == nil {
		return nil, fmt.Errorf("trace store, client, and logger are required")
	}
	if config.ChainID == 0 ||
		config.BatchSize <= 0 ||
		config.PollInterval <= 0 ||
		config.RequestTimeout <= 0 ||
		config.MinRequestInterval < 0 ||
		config.MaxAttempts == 0 ||
		config.RetryBase <= 0 ||
		config.RetryMax < config.RetryBase {
		return nil, fmt.Errorf("trace worker configuration is invalid")
	}
	return &Worker{store: store, client: client, logger: logger, config: config}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		processed, err := w.processBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger.Error("transaction trace batch failed", "error", err)
		} else if processed > 0 {
			w.logger.Info("transaction trace batch processed", "transactions", processed)
		}
		if wait(ctx, w.config.PollInterval) != nil {
			return nil
		}
	}
	return nil
}

func (w *Worker) processBatch(ctx context.Context) (int, error) {
	candidates, err := w.store.TraceCandidates(
		ctx,
		Version,
		w.config.ChainID,
		w.config.StartBlock,
		w.config.BatchSize,
	)
	if err != nil {
		return 0, fmt.Errorf("list transaction trace candidates: %w", err)
	}
	processed := 0
	for index, candidate := range candidates {
		if index > 0 && wait(ctx, w.config.MinRequestInterval) != nil {
			return processed, nil
		}
		attempt := candidate.AttemptCount + 1
		requestCtx, cancel := context.WithTimeout(ctx, w.config.RequestTimeout)
		root, raw, traceErr := w.client.TraceTransaction(
			requestCtx,
			candidate.TransactionHash,
		)
		cancel()
		if traceErr != nil {
			if err := w.recordFailure(ctx, candidate, attempt, traceErr); err != nil {
				return processed, err
			}
			w.logger.Warn(
				"transaction trace failed",
				"transaction_hash", candidate.TransactionHash,
				"attempt", attempt,
				"error", traceErr,
			)
			processed++
			continue
		}
		result, err := Analyze(candidate, root, raw, time.Now().UTC())
		if err != nil {
			if stateErr := w.recordFailure(ctx, candidate, attempt, err); stateErr != nil {
				return processed, stateErr
			}
			processed++
			continue
		}
		if err := w.store.InsertTrace(ctx, result); err != nil {
			return processed, fmt.Errorf(
				"insert transaction trace %s: %w",
				candidate.TransactionHash,
				err,
			)
		}
		processed++
	}
	return processed, nil
}

func (w *Worker) recordFailure(
	ctx context.Context,
	candidate Candidate,
	attempt uint32,
	cause error,
) error {
	status := "retry"
	nextRetryAt := time.Now().UTC().Add(w.retryDelay(attempt))
	if attempt >= w.config.MaxAttempts {
		status = "dead_letter"
		nextRetryAt = time.Unix(0, 0).UTC()
	}
	message := strings.TrimSpace(cause.Error())
	if len(message) > 2000 {
		message = message[:2000]
	}
	if err := w.store.RecordTraceFailure(
		ctx,
		candidate,
		Version,
		attempt,
		nextRetryAt,
		status,
		message,
	); err != nil {
		return fmt.Errorf(
			"record transaction trace failure %s: %w",
			candidate.TransactionHash,
			err,
		)
	}
	return nil
}

func (w *Worker) retryDelay(attempt uint32) time.Duration {
	delay := w.config.RetryBase
	for current := uint32(1); current < attempt; current++ {
		if delay >= w.config.RetryMax/2 {
			return w.config.RetryMax
		}
		delay *= 2
	}
	if delay > w.config.RetryMax {
		return w.config.RetryMax
	}
	return delay
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
