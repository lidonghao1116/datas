package alerting

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type Dispatcher struct {
	store        DeliveryStore
	sender       Sender
	logger       *slog.Logger
	workerID     string
	batchSize    int
	lease        time.Duration
	pollInterval time.Duration
	sendTimeout  time.Duration
	maxAttempts  int
	retryBase    time.Duration
	retryMax     time.Duration
}

func NewDispatcher(
	store DeliveryStore,
	sender Sender,
	logger *slog.Logger,
	workerID string,
	batchSize int,
	lease, pollInterval, sendTimeout time.Duration,
	maxAttempts int,
	retryBase, retryMax time.Duration,
) (*Dispatcher, error) {
	if store == nil || sender == nil {
		return nil, fmt.Errorf("delivery store and sender are required")
	}
	if workerID == "" || batchSize <= 0 || maxAttempts <= 0 {
		return nil, fmt.Errorf("worker ID, batch size, and max attempts are required")
	}
	if lease <= 0 || pollInterval <= 0 || sendTimeout <= 0 ||
		retryBase <= 0 || retryMax <= 0 {
		return nil, fmt.Errorf("dispatcher durations must be positive")
	}
	if retryMax < retryBase {
		return nil, fmt.Errorf("maximum retry delay cannot be less than base delay")
	}
	return &Dispatcher{
		store:        store,
		sender:       sender,
		logger:       logger,
		workerID:     workerID,
		batchSize:    batchSize,
		lease:        lease,
		pollInterval: pollInterval,
		sendTimeout:  sendTimeout,
		maxAttempts:  maxAttempts,
		retryBase:    retryBase,
		retryMax:     retryMax,
	}, nil
}

func (d *Dispatcher) Run(ctx context.Context) error {
	for {
		claimed, delivered, failed, err := d.process(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			d.logger.Error("alert delivery cycle failed", "error", err)
		} else if claimed > 0 {
			d.logger.Info(
				"alert delivery cycle complete",
				"claimed", claimed,
				"delivered", delivered,
				"failed", failed,
			)
		}
		if claimed < d.batchSize {
			if err := wait(ctx, d.pollInterval); err != nil {
				return nil
			}
		}
	}
}

func (d *Dispatcher) process(ctx context.Context) (int, int, int, error) {
	deliveries, err := d.store.ClaimDeliveries(ctx, d.workerID, d.batchSize, d.lease)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("claim deliveries: %w", err)
	}
	delivered := 0
	failed := 0
	for _, delivery := range deliveries {
		sendCtx, cancel := context.WithTimeout(ctx, d.sendTimeout)
		sendErr := d.sender.Send(sendCtx, delivery)
		cancel()
		if sendErr == nil {
			if err := d.store.MarkDelivered(ctx, delivery.Key, d.workerID); err != nil {
				return len(deliveries), delivered, failed, err
			}
			delivered++
			continue
		}
		deadLetter := delivery.Attempt >= d.maxAttempts
		nextAttempt := time.Now().UTC().Add(
			retryDelay(d.retryBase, d.retryMax, delivery.Attempt),
		)
		if deadLetter {
			nextAttempt = time.Now().UTC()
		}
		if err := d.store.MarkFailed(
			ctx,
			delivery.Key,
			d.workerID,
			sendErr.Error(),
			nextAttempt,
			deadLetter,
		); err != nil {
			return len(deliveries), delivered, failed, err
		}
		failed++
	}
	return len(deliveries), delivered, failed, nil
}

func retryDelay(base, maximum time.Duration, attempt int) time.Duration {
	delay := base
	for index := 1; index < attempt && delay < maximum; index++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}
