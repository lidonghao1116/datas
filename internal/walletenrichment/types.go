package walletenrichment

import (
	"context"
	"encoding/json"
	"time"
)

const SourceGMGN = "gmgn"

type Candidate struct {
	ChainID       uint64
	WalletAddress string
	Period        string
}

type Identity struct {
	DisplayName       string
	ENS               string
	PrimaryTag        string
	Tags              []string
	TwitterUsername   string
	TwitterName       string
	TwitterFollowers  uint64
	IsBlueVerified    bool
	CreatedTokenCount uint64
	WalletCreatedAt   time.Time
	FundFrom          string
	FundFromAddress   string
	FundAmountRaw     string
}

type Stats struct {
	WalletAddress       string
	Period              string
	NativeBalanceRaw    string
	RealizedProfitRaw   string
	UnrealizedProfitRaw string
	PnLRaw              string
	WinRateRaw          string
	TotalCostRaw        string
	BuyCount            uint64
	SellCount           uint64
	TokenCount          uint64
	AvgHoldingSeconds   uint64
	Identity            Identity
	SourceUpdatedAt     time.Time
	RawJSON             json.RawMessage
}

type Snapshot struct {
	ChainID   uint64
	Stats     Stats
	Source    string
	FetchedAt time.Time
	ExpiresAt time.Time
}

type SyncState struct {
	Source        string
	ChainID       uint64
	WalletAddress string
	Period        string
	Status        string
	LastError     string
	AttemptCount  uint32
	AttemptedAt   time.Time
	NextRetryAt   time.Time
}

type Client interface {
	WalletStats(
		ctx context.Context,
		chain string,
		walletAddress string,
		period string,
	) (Stats, error)
}

type Store interface {
	WalletEnrichmentCandidates(
		ctx context.Context,
		source string,
		period string,
		freshness time.Duration,
		activeLookback time.Duration,
		limit int,
	) ([]Candidate, error)
	InsertWalletEnrichmentSnapshot(ctx context.Context, snapshot Snapshot) error
	RecordWalletEnrichmentSync(ctx context.Context, state SyncState) error
}
