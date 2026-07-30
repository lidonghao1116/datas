package flashblocks

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/basewatch/base-analytics/internal/alerting"
)

func (w *Worker) runReconciler(ctx context.Context) {
	for ctx.Err() == nil {
		if err := w.reconcileBatch(ctx); err != nil && ctx.Err() == nil {
			w.logger.Error("Flashblocks reconciliation failed", "error", err)
		}
		if wait(ctx, w.config.ReconciliationPoll) != nil {
			return
		}
	}
}

func (w *Worker) reconcileBatch(ctx context.Context) error {
	items, err := w.state.PendingReconciliations(
		ctx,
		w.config.ReconciliationBatch,
	)
	if err != nil {
		return fmt.Errorf("list pending preconfirmations: %w", err)
	}
	for _, item := range items {
		if err := w.reconcile(ctx, item); err != nil {
			nextCheck := time.Now().UTC().Add(w.config.ReconciliationPoll)
			if deferErr := w.state.DeferReconciliation(
				ctx,
				item.Key,
				nextCheck,
				err.Error(),
			); deferErr != nil {
				w.logger.Error(
					"defer Flashblocks reconciliation",
					"preconfirmation_key", item.Key,
					"error", deferErr,
				)
			}
		}
	}
	return nil
}

func (w *Worker) reconcile(
	ctx context.Context,
	item Reconciliation,
) error {
	requestCtx, cancel := context.WithTimeout(ctx, w.config.RequestTimeout)
	receipt, found, err := w.source.ReceiptByHash(requestCtx, item.TransactionHash)
	cancel()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if !found {
		if now.Before(item.ExpiresAt) {
			return w.state.DeferReconciliation(
				ctx,
				item.Key,
				now.Add(w.config.ReconciliationPoll),
				"",
			)
		}
		return w.resolve(ctx, item, "expired", now, 0, "")
	}
	blockNumber, err := hexutil.DecodeUint64(receipt.BlockNumber)
	if err != nil {
		return fmt.Errorf("decode canonical receipt block number: %w", err)
	}
	status := "reverted"
	if receipt.Status == "0x1" && receiptContains(receipt, item) {
		status = "confirmed"
	}
	return w.resolve(ctx, item, status, now, blockNumber, strings.ToLower(receipt.BlockHash))
}

func (w *Worker) resolve(
	ctx context.Context,
	item Reconciliation,
	status string,
	resolvedAt time.Time,
	blockNumber uint64,
	blockHash string,
) error {
	payload, err := json.Marshal(map[string]any{
		"lifecycle":            status,
		"preconfirmation_key":  item.Key,
		"transaction_hash":     item.TransactionHash,
		"log_index":            item.LogIndex,
		"pool_address":         item.PoolAddress,
		"preconfirmed_block":   item.BlockNumber,
		"preconfirmed_hash":    item.BlockHash,
		"canonical_block":      blockNumber,
		"canonical_block_hash": blockHash,
		"pending_alert_keys":   item.AlertKeys,
		"observed_at":          item.ObservedAt,
		"resolved_at":          resolvedAt,
	})
	if err != nil {
		return fmt.Errorf("encode reconciliation alert: %w", err)
	}
	severity := "medium"
	if status == "reverted" {
		severity = "critical"
	}
	alert := alerting.Alert{
		Key:             fmt.Sprintf("%s:%s:%s", AlertVersion, item.Key, status),
		Type:            "preconfirmation_" + status,
		Severity:        severity,
		ChainID:         item.ChainID,
		BlockNumber:     blockNumber,
		TransactionHash: item.TransactionHash,
		TokenAddress:    item.PoolAddress,
		Title:           fmt.Sprintf("preconfirmation %s: %s", status, item.TransactionHash),
		Payload:         payload,
	}
	return w.state.Resolve(
		ctx,
		item,
		status,
		resolvedAt,
		blockNumber,
		blockHash,
		alert,
	)
}

func receiptContains(receipt Receipt, item Reconciliation) bool {
	for _, log := range receipt.Logs {
		logIndex, err := hexutil.DecodeUint64(log.LogIndex)
		if err != nil || logIndex > uint64(^uint32(0)) {
			continue
		}
		if uint32(logIndex) == item.LogIndex &&
			strings.EqualFold(log.Address, item.PoolAddress) &&
			!log.Removed {
			return true
		}
	}
	return false
}
