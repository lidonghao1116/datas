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
	Key             string
	Type            string
	Severity        string
	ChainID         uint64
	BlockNumber     uint64
	TransactionHash string
	TokenAddress    string
	TokenSymbol     string
	Title           string
	Payload         json.RawMessage
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
