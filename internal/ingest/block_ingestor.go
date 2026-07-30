package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
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
	maxReorgDepth  uint64
	reconnectDelay time.Duration
	logger         *slog.Logger
}

func NewBlockIngestor(
	chain ChainClient,
	publisher messaging.RawBlockPublisher,
	checkpoints checkpoint.Store,
	chainID uint64,
	startBlock uint64,
	maxReorgDepth uint64,
	reconnectDelay time.Duration,
	logger *slog.Logger,
) *BlockIngestor {
	return &BlockIngestor{
		chain:          chain,
		publisher:      publisher,
		checkpoints:    checkpoints,
		chainID:        chainID,
		startBlock:     startBlock,
		maxReorgDepth:  maxReorgDepth,
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
		return last.BlockNumber + 1, nil
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
		reorganization, err := i.detectReorganization(ctx, envelope)
		if err != nil {
			return err
		}
		if reorganization != nil {
			replacementNumber := reorganization.CommonAncestor.BlockNumber + 1
			replacement := envelope
			if replacement.BlockNumber != replacementNumber {
				replacement, err = i.chain.FetchBlock(ctx, replacementNumber)
				if err != nil {
					return fmt.Errorf("fetch replacement block %d: %w", replacementNumber, err)
				}
			}
			replacement.Reorganization = reorganization
			if err := i.publisher.Publish(ctx, replacement); err != nil {
				return err
			}
			if err := i.checkpoints.Rewind(
				ctx,
				PipelineName,
				i.chainID,
				checkpoint.Point{
					BlockNumber: reorganization.CommonAncestor.BlockNumber,
					BlockHash:   reorganization.CommonAncestor.BlockHash,
				},
			); err != nil {
				return err
			}
			*next = replacementNumber
			if err := i.saveCheckpoint(ctx, replacement); err != nil {
				return err
			}
			i.logger.Warn(
				"chain reorganization detected",
				"common_ancestor", reorganization.CommonAncestor.BlockNumber,
				"old_head", reorganization.OldHead.BlockNumber,
				"orphaned_blocks", len(reorganization.OrphanedBlocks),
			)
			i.logPublished(replacement)
			(*next)++
			continue
		}
		if err := i.publisher.Publish(ctx, envelope); err != nil {
			return err
		}
		if err := i.saveCheckpoint(ctx, envelope); err != nil {
			return err
		}
		i.logPublished(envelope)
		(*next)++
	}
	return nil
}

func (i *BlockIngestor) saveCheckpoint(
	ctx context.Context,
	envelope domain.RawBlockEnvelope,
) error {
	return i.checkpoints.Save(
		ctx,
		PipelineName,
		i.chainID,
		envelope.BlockNumber,
		envelope.BlockHash,
	)
}

func (i *BlockIngestor) logPublished(envelope domain.RawBlockEnvelope) {
	i.logger.Info(
		"block published",
		"block_number", envelope.BlockNumber,
		"block_hash", envelope.BlockHash,
		"receipt_count", len(envelope.Receipts),
	)
}

func (i *BlockIngestor) detectReorganization(
	ctx context.Context,
	envelope domain.RawBlockEnvelope,
) (*domain.ChainReorganization, error) {
	if envelope.BlockNumber == 0 {
		return nil, nil
	}
	previousNumber := envelope.BlockNumber - 1
	storedParent, exists, err := i.checkpoints.Header(
		ctx,
		PipelineName,
		i.chainID,
		previousNumber,
	)
	if err != nil {
		return nil, err
	}
	if !exists || equalHash(storedParent.BlockHash, envelope.ParentHash) {
		return nil, nil
	}

	oldHead, exists, err := i.checkpoints.Load(ctx, PipelineName, i.chainID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("parent hash mismatch without an ingestion checkpoint")
	}

	candidateHash := envelope.ParentHash
	height := previousNumber
	for depth := uint64(0); depth <= i.maxReorgDepth; depth++ {
		stored, found, err := i.checkpoints.Header(ctx, PipelineName, i.chainID, height)
		if err != nil {
			return nil, err
		}
		if found && equalHash(stored.BlockHash, candidateHash) {
			orphaned, err := i.checkpoints.Range(
				ctx,
				PipelineName,
				i.chainID,
				height+1,
				oldHead.BlockNumber,
			)
			if err != nil {
				return nil, err
			}
			if len(orphaned) == 0 {
				return nil, fmt.Errorf("reorganization at block %d has no orphaned history", envelope.BlockNumber)
			}
			references := make([]domain.BlockReference, 0, len(orphaned))
			for _, point := range orphaned {
				references = append(references, domain.BlockReference{
					BlockNumber: point.BlockNumber,
					BlockHash:   point.BlockHash,
				})
			}
			return &domain.ChainReorganization{
				CommonAncestor: domain.BlockReference{
					BlockNumber: stored.BlockNumber,
					BlockHash:   stored.BlockHash,
				},
				OldHead: domain.BlockReference{
					BlockNumber: oldHead.BlockNumber,
					BlockHash:   oldHead.BlockHash,
				},
				OrphanedBlocks: references,
				DetectedAt:     time.Now().UTC(),
			}, nil
		}
		if height == 0 || depth == i.maxReorgDepth {
			break
		}
		candidate, err := i.chain.FetchBlock(ctx, height)
		if err != nil {
			return nil, fmt.Errorf("fetch candidate ancestor block %d: %w", height, err)
		}
		candidateHash = candidate.ParentHash
		height--
	}
	return nil, fmt.Errorf(
		"reorganization exceeds maximum depth %d at block %d",
		i.maxReorgDepth,
		envelope.BlockNumber,
	)
}

func equalHash(left, right string) bool {
	return strings.EqualFold(strings.TrimSpace(left), strings.TrimSpace(right))
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
