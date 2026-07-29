package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/basewatch/base-analytics/internal/chain/base"
	"github.com/basewatch/base-analytics/internal/checkpoint"
	"github.com/basewatch/base-analytics/internal/domain"
	"github.com/basewatch/base-analytics/internal/messaging"
)

const PipelineName = "standard-blocks-v1"

type ChainClient interface {
	LatestBlockNumber(ctx context.Context) (uint64, error)
	FetchBlock(ctx context.Context, number uint64) (domain.RawBlockEnvelope, error)
	SubscribeHeads(ctx context.Context) (<-chan base.Head, <-chan error, func(), error)
}

type BlockIngestor struct {
	chain          ChainClient
	publisher      messaging.RawBlockPublisher
	checkpoints    checkpoint.Store
	chainID        uint64
	startBlock     uint64
	reconnectDelay time.Duration
	logger         *slog.Logger
}

func NewBlockIngestor(
	chain ChainClient,
	publisher messaging.RawBlockPublisher,
	checkpoints checkpoint.Store,
	chainID uint64,
	startBlock uint64,
	reconnectDelay time.Duration,
	logger *slog.Logger,
) *BlockIngestor {
	return &BlockIngestor{
		chain:          chain,
		publisher:      publisher,
		checkpoints:    checkpoints,
		chainID:        chainID,
		startBlock:     startBlock,
		reconnectDelay: reconnectDelay,
		logger:         logger,
	}
}

func (i *BlockIngestor) Run(ctx context.Context) error {
	next, err := i.nextBlock(ctx)
	if err != nil {
		return err
	}
	i.logger.Info("block ingestion starting", "chain_id", i.chainID, "next_block", next)

	subscriptionsEnabled := true
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		latest, err := i.chain.LatestBlockNumber(ctx)
		if err != nil {
			i.logger.Warn("failed to read chain head", "error", err)
			if !wait(ctx, i.reconnectDelay) {
				return nil
			}
			continue
		}
		if err := i.processRange(ctx, &next, latest); err != nil {
			i.logger.Warn("failed to process block range; retrying", "next_block", next, "error", err)
			if !wait(ctx, i.reconnectDelay) {
				return nil
			}
			continue
		}

		if !subscriptionsEnabled {
			if !wait(ctx, i.reconnectDelay) {
				return nil
			}
			continue
		}

		heads, subscriptionErrors, closeSubscription, err := i.chain.SubscribeHeads(ctx)
		if err != nil {
			if base.SubscriptionUnsupported(err) {
				subscriptionsEnabled = false
				i.logger.Info("newHeads subscriptions unavailable; using HTTP polling", "error", err)
				continue
			}
			i.logger.Warn("failed to subscribe to newHeads", "error", err)
			if !wait(ctx, i.reconnectDelay) {
				return nil
			}
			continue
		}

		reconnect := false
		for !reconnect {
			select {
			case <-ctx.Done():
				closeSubscription()
				return nil
			case err, ok := <-subscriptionErrors:
				if ok && err != nil {
					i.logger.Warn("newHeads subscription ended", "error", err)
				}
				reconnect = true
			case head, ok := <-heads:
				if !ok {
					reconnect = true
					continue
				}
				if err := i.processRange(ctx, &next, uint64(head.Number)); err != nil {
					i.logger.Warn("failed to process block range; reconnecting", "next_block", next, "error", err)
					reconnect = true
				}
			}
		}
		closeSubscription()
		if !wait(ctx, i.reconnectDelay) {
			return nil
		}
	}
}

func (i *BlockIngestor) nextBlock(ctx context.Context) (uint64, error) {
	last, exists, err := i.checkpoints.Load(ctx, PipelineName, i.chainID)
	if err != nil {
		return 0, err
	}
	if exists {
		return last + 1, nil
	}
	if i.startBlock > 0 {
		return i.startBlock, nil
	}
	latest, err := i.chain.LatestBlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("resolve initial chain head: %w", err)
	}
	return latest, nil
}

func (i *BlockIngestor) processRange(ctx context.Context, next *uint64, target uint64) error {
	for *next <= target {
		envelope, err := i.chain.FetchBlock(ctx, *next)
		if err != nil {
			return fmt.Errorf("fetch block %d: %w", *next, err)
		}
		if err := i.publisher.Publish(ctx, envelope); err != nil {
			return err
		}
		if err := i.checkpoints.Save(
			ctx,
			PipelineName,
			i.chainID,
			envelope.BlockNumber,
			envelope.BlockHash,
		); err != nil {
			return err
		}
		i.logger.Info(
			"block published",
			"block_number", envelope.BlockNumber,
			"block_hash", envelope.BlockHash,
			"receipt_count", len(envelope.Receipts),
		)
		(*next)++
	}
	return nil
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
