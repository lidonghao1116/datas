package flashblocks

import (
	"context"
	"encoding/json"
	"time"

	"github.com/basewatch/base-analytics/internal/alerting"
	"github.com/basewatch/base-analytics/internal/domain"
	"github.com/basewatch/base-analytics/internal/valuation"
)

const AlertVersion = "flash-v1"

type PendingLog struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockHash        string   `json:"blockHash"`
	BlockNumber      string   `json:"blockNumber"`
	BlockTimestamp   string   `json:"blockTimestamp"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

type Transaction struct {
	Hash string `json:"hash"`
	From string `json:"from"`
	To   string `json:"to"`
}

type Receipt struct {
	Status          string       `json:"status"`
	BlockHash       string       `json:"blockHash"`
	BlockNumber     string       `json:"blockNumber"`
	TransactionHash string       `json:"transactionHash"`
	Logs            []PendingLog `json:"logs"`
}

type Enrichment struct {
	Valuation       valuation.Candidate
	SmartScore      string
	SmartGrade      string
	SmartConfidence string
	SmartVersion    string
	SmartSourceAt   time.Time
	SmartCalculated time.Time
	Token0Risk      alerting.RiskFlags
	Token1Risk      alerting.RiskFlags
}

type Preconfirmation struct {
	Key             string
	ChainID         uint64
	TransactionHash string
	LogIndex        uint32
	PoolAddress     string
	BlockNumber     uint64
	BlockHash       string
	ObservedAt      time.Time
	ExpiresAt       time.Time
	AlertKeys       []string
	Payload         json.RawMessage
}

type Reconciliation struct {
	Preconfirmation
	AttemptCount uint32
}

type Source interface {
	SubscribePendingLogs(
		ctx context.Context,
		topics []string,
	) (<-chan PendingLog, <-chan error, func(), error)
	PendingSnapshot(
		ctx context.Context,
		topics []string,
	) ([]PendingLog, map[string]Transaction, error)
	TransactionByHash(ctx context.Context, hash string) (Transaction, bool, error)
	ReceiptByHash(ctx context.Context, hash string) (Receipt, bool, error)
}

type EnrichmentStore interface {
	EnrichPendingSwap(
		ctx context.Context,
		swap domain.PoolSwap,
		walletAddress string,
		scoreVersion string,
	) (Enrichment, bool, error)
}

type StateStore interface {
	InsertPending(
		ctx context.Context,
		preconfirmation Preconfirmation,
		alerts []alerting.Alert,
	) (bool, error)
	PendingByKey(ctx context.Context, key string) (Reconciliation, bool, error)
	PendingReconciliations(ctx context.Context, limit int) ([]Reconciliation, error)
	DeferReconciliation(
		ctx context.Context,
		key string,
		nextCheckAt time.Time,
		lastError string,
	) error
	Resolve(
		ctx context.Context,
		preconfirmation Reconciliation,
		status string,
		resolvedAt time.Time,
		blockNumber uint64,
		blockHash string,
		alert alerting.Alert,
	) error
}
