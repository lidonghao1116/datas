package walletprofile

import (
	"context"
	"time"
)

const AttributionTransactionFrom = "transaction_from"

type Candidate struct {
	EventID              string
	ValuationVersion     string
	ChainID              uint64
	WalletAddress        string
	RouterAddress        string
	BlockNumber          uint64
	BlockTime            time.Time
	TransactionHash      string
	TransactionIndex     uint32
	LogIndex             uint32
	PoolAddress          string
	Protocol             string
	ProtocolVersion      string
	BoughtTokenAddress   string
	BoughtTokenSymbol    string
	BoughtTokenAmountRaw string
	SoldTokenAddress     string
	SoldTokenSymbol      string
	SoldTokenAmountRaw   string
	TradeValueUSDRaw     string
	ValuationStatus      string
	SourceValuedAt       time.Time
}

type Activity struct {
	Candidate
	AttributionMethod string
	GeneratedAt       time.Time
}

type Store interface {
	WalletActivityCandidates(ctx context.Context, limit int) ([]Candidate, error)
	InsertWalletActivities(ctx context.Context, activities []Activity) error
}
