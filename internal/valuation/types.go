package valuation

import (
	"context"
	"time"

	"github.com/basewatch/base-analytics/internal/domain"
)

const Version = "usd-v2"

type Price struct {
	Raw             string
	Source          string
	SourceUpdatedAt time.Time
	FetchedAt       time.Time
}

type Candidate struct {
	Swap   domain.PoolSwap
	Price0 Price
	Price1 Price
}

type Result struct {
	EventID              string
	Swap                 domain.PoolSwap
	Token0AmountRaw      string
	Token1AmountRaw      string
	Token0PriceUSDRaw    string
	Token1PriceUSDRaw    string
	Token0ValueUSDRaw    string
	Token1ValueUSDRaw    string
	TradeValueUSDRaw     string
	BoughtTokenAddress   string
	BoughtTokenSymbol    string
	SoldTokenAddress     string
	SoldTokenSymbol      string
	Status               string
	Version              string
	IsLargeTrade         uint8
	PriceSource          string
	Token0PriceUpdatedAt time.Time
	Token1PriceUpdatedAt time.Time
	ValuedAt             time.Time
}

type Store interface {
	ValuationCandidates(
		ctx context.Context,
		lookback time.Duration,
		maxPriceAge time.Duration,
		limit int,
	) ([]Candidate, error)
	InsertValuations(ctx context.Context, results []Result) error
}
