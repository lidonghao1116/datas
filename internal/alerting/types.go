package alerting

import (
	"context"
	"encoding/json"
	"time"
)

type RiskFlags struct {
	IsHoneypot     bool `json:"is_honeypot"`
	HasMintMethod  bool `json:"has_mint_method"`
	HasBlackMethod bool `json:"has_black_method"`
	IsProxy        bool `json:"is_proxy"`
}

type Candidate struct {
	EventID            string
	ValuationVersion   string
	ChainID            uint64
	BlockNumber        uint64
	BlockTime          time.Time
	TransactionHash    string
	PoolAddress        string
	Protocol           string
	ProtocolVersion    string
	BoughtTokenAddress string
	BoughtTokenSymbol  string
	SoldTokenAddress   string
	SoldTokenSymbol    string
	TradeValueUSDRaw   string
	ValuedAt           time.Time
	BoughtRisk         RiskFlags
	SoldRisk           RiskFlags
}

type Alert struct {
	Key             string          `json:"alert_key"`
	Type            string          `json:"alert_type"`
	Severity        string          `json:"severity"`
	ChainID         uint64          `json:"chain_id"`
	BlockNumber     uint64          `json:"block_number"`
	TransactionHash string          `json:"transaction_hash"`
	TokenAddress    string          `json:"token_address"`
	TokenSymbol     string          `json:"token_symbol"`
	Title           string          `json:"title"`
	Payload         json.RawMessage `json:"payload"`
}

type Delivery struct {
	Alert
	Attempt   int       `json:"attempt"`
	CreatedAt time.Time `json:"created_at"`
}

type DeliveryStore interface {
	ClaimDeliveries(
		ctx context.Context,
		workerID string,
		limit int,
		lease time.Duration,
	) ([]Delivery, error)
	MarkDelivered(ctx context.Context, alertKey, workerID string) error
	MarkFailed(
		ctx context.Context,
		alertKey, workerID, lastError string,
		nextAttemptAt time.Time,
		deadLetter bool,
	) error
}

type Sender interface {
	Send(ctx context.Context, delivery Delivery) error
}

type Cursor struct {
	ValuedAt time.Time
	EventID  string
}

type CandidateStore interface {
	LargeTradeCandidates(
		ctx context.Context,
		lookback time.Duration,
		after Cursor,
		limit int,
	) ([]Candidate, error)
}

type Outbox interface {
	InsertAlerts(ctx context.Context, alerts []Alert) (int, error)
}
