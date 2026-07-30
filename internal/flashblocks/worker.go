package flashblocks

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/basewatch/base-analytics/internal/alerting"
	"github.com/basewatch/base-analytics/internal/domain"
	parserlogs "github.com/basewatch/base-analytics/internal/parser/logs"
	"github.com/basewatch/base-analytics/internal/valuation"
)

type AlertBuilder interface {
	BuildAlerts(candidate alerting.Candidate) ([]alerting.Alert, error)
}

type WorkerConfig struct {
	ChainID              uint64
	ScoreVersion         string
	ReconciliationTTL    time.Duration
	ReconciliationBatch  int
	ReconciliationPoll   time.Duration
	ReconnectDelay       time.Duration
	RequestTimeout       time.Duration
	FallbackPollInterval time.Duration
}

type Worker struct {
	source     Source
	enrichment EnrichmentStore
	state      StateStore
	alerts     AlertBuilder
	parser     *parserlogs.Parser
	calculator *valuation.Calculator
	logger     *slog.Logger
	config     WorkerConfig
	topics     []string
	pollSeen   map[string]time.Time
}

func NewWorker(
	source Source,
	enrichment EnrichmentStore,
	state StateStore,
	alerts AlertBuilder,
	parser *parserlogs.Parser,
	calculator *valuation.Calculator,
	logger *slog.Logger,
	config WorkerConfig,
) (*Worker, error) {
	if source == nil || enrichment == nil || state == nil || alerts == nil ||
		parser == nil || calculator == nil || logger == nil {
		return nil, fmt.Errorf("Flashblocks worker dependencies are required")
	}
	if config.ChainID == 0 || strings.TrimSpace(config.ScoreVersion) == "" ||
		config.ReconciliationTTL <= 0 || config.ReconciliationBatch <= 0 ||
		config.ReconciliationPoll <= 0 || config.ReconnectDelay <= 0 ||
		config.RequestTimeout <= 0 || config.FallbackPollInterval <= 0 {
		return nil, fmt.Errorf("Flashblocks worker configuration is invalid")
	}
	return &Worker{
		source:     source,
		enrichment: enrichment,
		state:      state,
		alerts:     alerts,
		parser:     parser,
		calculator: calculator,
		logger:     logger,
		config:     config,
		pollSeen:   make(map[string]time.Time),
		topics: []string{
			parserlogs.V2SwapDecoder{}.Topic0(),
			parserlogs.V3SwapDecoder{}.Topic0(),
		},
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	go w.runReconciler(ctx)
	for ctx.Err() == nil {
		if err := w.runSubscription(ctx); err != nil && ctx.Err() == nil {
			w.logger.Warn("Flashblocks subscription ended", "error", err)
		}
		err := w.runPollingFallback(ctx, w.config.ReconnectDelay)
		if err != nil && ctx.Err() == nil && err != context.DeadlineExceeded {
			w.logger.Warn("Flashblocks polling fallback failed", "error", err)
			if wait(ctx, w.config.ReconnectDelay) != nil {
				break
			}
		}
	}
	return nil
}

func (w *Worker) runPollingFallback(ctx context.Context, duration time.Duration) error {
	ticker := time.NewTicker(w.config.FallbackPollInterval)
	defer ticker.Stop()
	reconnect := time.NewTimer(duration)
	defer reconnect.Stop()
	for {
		if err := w.pollPendingLogs(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-reconnect.C:
			return nil
		case <-ticker.C:
		}
	}
}

func (w *Worker) pollPendingLogs(ctx context.Context) error {
	requestCtx, cancel := context.WithTimeout(ctx, w.config.RequestTimeout)
	pendingLogs, transactions, err := w.source.PendingSnapshot(requestCtx, w.topics)
	cancel()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for key, seenAt := range w.pollSeen {
		if now.Sub(seenAt) > w.config.ReconciliationTTL {
			delete(w.pollSeen, key)
		}
	}
	type handledLog struct {
		key string
		at  time.Time
	}
	handled := make(chan handledLog, len(pendingLogs))
	sem := make(chan struct{}, 8)
	var workers sync.WaitGroup
	for _, pendingLog := range pendingLogs {
		key := pendingLog.TransactionHash + ":" + pendingLog.LogIndex
		if _, seen := w.pollSeen[key]; seen {
			continue
		}
		transaction, found := transactions[strings.ToLower(pendingLog.TransactionHash)]
		if !found {
			continue
		}
		workers.Add(1)
		go func(pendingLog PendingLog, transaction Transaction, key string) {
			defer workers.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			if err := w.handlePendingLogWithTransaction(ctx, pendingLog, transaction); err != nil {
				w.logger.Warn(
					"skip polled Flashblocks log",
					"transaction_hash", pendingLog.TransactionHash,
					"log_index", pendingLog.LogIndex,
					"error", err,
				)
				return
			}
			handled <- handledLog{key: key, at: now}
		}(pendingLog, transaction, key)
	}
	workers.Wait()
	close(handled)
	for item := range handled {
		w.pollSeen[item.key] = item.at
	}
	return nil
}

func (w *Worker) runSubscription(ctx context.Context) error {
	pendingLogs, subscriptionErrors, closeSubscription, err :=
		w.source.SubscribePendingLogs(ctx, w.topics)
	if err != nil {
		return err
	}
	defer closeSubscription()
	w.logger.Info("Flashblocks pendingLogs subscription active")
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-subscriptionErrors:
			if err == nil {
				return fmt.Errorf("pendingLogs subscription closed")
			}
			return err
		case pendingLog, ok := <-pendingLogs:
			if !ok {
				return fmt.Errorf("pendingLogs channel closed")
			}
			if err := w.handlePendingLog(ctx, pendingLog); err != nil {
				w.logger.Warn(
					"skip Flashblocks pending log",
					"transaction_hash", pendingLog.TransactionHash,
					"log_index", pendingLog.LogIndex,
					"error", err,
				)
			}
		}
	}
}

func (w *Worker) handlePendingLog(ctx context.Context, pendingLog PendingLog) error {
	if pendingLog.Removed {
		return w.handlePendingLogWithTransaction(ctx, pendingLog, Transaction{})
	}
	requestCtx, cancel := context.WithTimeout(ctx, w.config.RequestTimeout)
	transaction, found, err := w.source.TransactionByHash(requestCtx, pendingLog.TransactionHash)
	cancel()
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("preconfirmed transaction is not available")
	}
	return w.handlePendingLogWithTransaction(ctx, pendingLog, transaction)
}

func (w *Worker) handlePendingLogWithTransaction(
	ctx context.Context,
	pendingLog PendingLog,
	transaction Transaction,
) error {
	meta, rawLog, key, err := w.normalizedLog(pendingLog)
	if err != nil {
		return err
	}
	if pendingLog.Removed {
		item, found, err := w.state.PendingByKey(ctx, key)
		if err != nil || !found {
			return err
		}
		return w.resolve(ctx, item, "reverted", time.Now().UTC(), 0, "")
	}
	swap, matched, err := w.parser.ParseSwap(meta, rawLog)
	if err != nil || !matched {
		return err
	}
	if !common.IsHexAddress(transaction.From) {
		return fmt.Errorf("preconfirmed transaction is not available")
	}
	walletAddress := strings.ToLower(common.HexToAddress(transaction.From).Hex())
	enrichment, found, err := w.enrichment.EnrichPendingSwap(
		ctx,
		swap,
		walletAddress,
		w.config.ScoreVersion,
	)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	result, valued, err := w.calculator.Value(enrichment.Valuation)
	if err != nil || !valued || result.Status != "valued" {
		return err
	}
	candidate := w.alertCandidate(result, enrichment, walletAddress, key)
	alerts, err := w.alerts.BuildAlerts(candidate)
	if err != nil {
		return err
	}
	if len(alerts) == 0 {
		return nil
	}
	alertKeys := make([]string, 0, len(alerts))
	for index := range alerts {
		alerts[index] = preconfirmedAlert(key, alerts[index], meta.ObservedAt)
		alertKeys = append(alertKeys, alerts[index].Key)
	}
	payload, err := json.Marshal(pendingLog)
	if err != nil {
		return fmt.Errorf("encode pending log: %w", err)
	}
	inserted, err := w.state.InsertPending(ctx, Preconfirmation{
		Key:             key,
		ChainID:         w.config.ChainID,
		TransactionHash: swap.TransactionHash,
		LogIndex:        swap.LogIndex,
		PoolAddress:     swap.PoolAddress,
		BlockNumber:     swap.BlockNumber,
		BlockHash:       swap.BlockHash,
		ObservedAt:      meta.ObservedAt,
		ExpiresAt:       meta.ObservedAt.Add(w.config.ReconciliationTTL),
		AlertKeys:       alertKeys,
		Payload:         payload,
	}, alerts)
	if err != nil {
		return err
	}
	if inserted {
		w.logger.Info(
			"preconfirmed alert created",
			"transaction_hash", swap.TransactionHash,
			"alerts", len(alerts),
		)
	}
	return nil
}

func (w *Worker) alertCandidate(
	result valuation.Result,
	enrichment Enrichment,
	walletAddress string,
	key string,
) alerting.Candidate {
	boughtRisk := enrichment.Token0Risk
	soldRisk := enrichment.Token1Risk
	if result.BoughtTokenAddress == result.Swap.Token1Address {
		boughtRisk, soldRisk = enrichment.Token1Risk, enrichment.Token0Risk
	}
	return alerting.Candidate{
		EventID:            key,
		ValuationVersion:   AlertVersion,
		ChainID:            w.config.ChainID,
		WalletAddress:      walletAddress,
		AttributionMethod:  "preconfirmed_transaction_from",
		BlockNumber:        result.Swap.BlockNumber,
		BlockTime:          result.Swap.BlockTime,
		TransactionHash:    result.Swap.TransactionHash,
		PoolAddress:        result.Swap.PoolAddress,
		Protocol:           result.Swap.Protocol,
		ProtocolVersion:    result.Swap.ProtocolVersion,
		BoughtTokenAddress: result.BoughtTokenAddress,
		BoughtTokenSymbol:  result.BoughtTokenSymbol,
		SoldTokenAddress:   result.SoldTokenAddress,
		SoldTokenSymbol:    result.SoldTokenSymbol,
		TradeValueUSDRaw:   result.TradeValueUSDRaw,
		ValuedAt:           result.ValuedAt,
		ObservedAt:         result.Swap.ObservedAt,
		IsLargeTrade:       result.IsLargeTrade == 1,
		SmartScoreVersion:  enrichment.SmartVersion,
		SmartScoreRaw:      enrichment.SmartScore,
		SmartScoreGrade:    enrichment.SmartGrade,
		SmartConfidenceRaw: enrichment.SmartConfidence,
		SmartScoreSourceAt: enrichment.SmartSourceAt,
		SmartScoreAt:       enrichment.SmartCalculated,
		BoughtRisk:         boughtRisk,
		SoldRisk:           soldRisk,
	}
}

func (w *Worker) normalizedLog(
	pendingLog PendingLog,
) (domain.EventMeta, parserlogs.RawLog, string, error) {
	blockNumber, err := hexutil.DecodeUint64(pendingLog.BlockNumber)
	if err != nil {
		return domain.EventMeta{}, parserlogs.RawLog{}, "", fmt.Errorf("invalid block number: %w", err)
	}
	blockTimestamp := uint64(time.Now().UTC().Unix())
	if pendingLog.BlockTimestamp != "" {
		blockTimestamp, err = hexutil.DecodeUint64(pendingLog.BlockTimestamp)
		if err != nil {
			return domain.EventMeta{}, parserlogs.RawLog{}, "", fmt.Errorf("invalid block timestamp: %w", err)
		}
	}
	transactionIndex, err := hexutil.DecodeUint64(pendingLog.TransactionIndex)
	if err != nil {
		return domain.EventMeta{}, parserlogs.RawLog{}, "", fmt.Errorf("invalid transaction index: %w", err)
	}
	if transactionIndex > uint64(^uint32(0)) {
		return domain.EventMeta{}, parserlogs.RawLog{}, "", fmt.Errorf("transaction index exceeds uint32")
	}
	logIndex, err := hexutil.DecodeUint64(pendingLog.LogIndex)
	if err != nil || logIndex > uint64(^uint32(0)) {
		return domain.EventMeta{}, parserlogs.RawLog{}, "", fmt.Errorf("invalid log index")
	}
	transactionHash, hashErr := hexutil.Decode(pendingLog.TransactionHash)
	if !common.IsHexAddress(pendingLog.Address) ||
		hashErr != nil ||
		len(transactionHash) != common.HashLength {
		return domain.EventMeta{}, parserlogs.RawLog{}, "", fmt.Errorf("invalid pending log identity")
	}
	observedAt := time.Now().UTC()
	meta := domain.EventMeta{
		SchemaVersion:    domain.NormalizedEventSchemaVersion,
		ChainID:          w.config.ChainID,
		BlockNumber:      blockNumber,
		BlockHash:        strings.ToLower(pendingLog.BlockHash),
		BlockTime:        time.Unix(int64(blockTimestamp), 0).UTC(),
		TransactionHash:  strings.ToLower(pendingLog.TransactionHash),
		TransactionIndex: uint32(transactionIndex),
		LogIndex:         uint32(logIndex),
		ObservedAt:       observedAt,
		IsCanonical:      0,
		ParserVersion:    parserlogs.ParserVersion,
	}
	rawLog := parserlogs.RawLog{
		Address:          strings.ToLower(pendingLog.Address),
		Topics:           pendingLog.Topics,
		Data:             pendingLog.Data,
		TransactionHash:  meta.TransactionHash,
		TransactionIndex: hexutil.Uint64(transactionIndex),
		LogIndex:         hexutil.Uint64(logIndex),
		Removed:          pendingLog.Removed,
	}
	key := fmt.Sprintf("%d:%s:%d", w.config.ChainID, meta.TransactionHash, logIndex)
	return meta, rawLog, key, nil
}

func preconfirmedAlert(
	preconfirmationKey string,
	alert alerting.Alert,
	observedAt time.Time,
) alerting.Alert {
	originalType := alert.Type
	alert.Type = "preconfirmed_" + originalType
	alert.Key = fmt.Sprintf(
		"%s:%s:%s:%s",
		AlertVersion,
		preconfirmationKey,
		alert.Type,
		strings.ToLower(alert.TokenAddress),
	)
	alert.Title = "preconfirmed " + alert.Title
	var signal any
	if err := json.Unmarshal(alert.Payload, &signal); err != nil {
		signal = map[string]any{}
	}
	alert.Payload, _ = json.Marshal(map[string]any{
		"lifecycle":           "pending",
		"preconfirmation_key": preconfirmationKey,
		"observed_at":         observedAt,
		"signal":              signal,
		"original_type":       originalType,
	})
	return alert
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
