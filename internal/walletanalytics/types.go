package walletanalytics

import (
	"context"
	"time"
)

const Version = "smart-v1"

type Candidate struct {
	ChainID       uint64
	WalletAddress string
}

type Trade struct {
	EventID            string
	BlockTime          time.Time
	BoughtTokenAddress string
	BoughtTokenSymbol  string
	BoughtAmountRaw    string
	SoldTokenAddress   string
	SoldTokenSymbol    string
	SoldAmountRaw      string
	TradeValueUSDRaw   string
	ValuationStatus    string
	GeneratedAt        time.Time
}

type Price struct {
	Raw       string
	UpdatedAt time.Time
}

type Risk struct {
	IsHoneypot     bool
	HasMintMethod  bool
	HasBlackMethod bool
	IsProxy        bool
}

func (r Risk) Risky() bool {
	return r.IsHoneypot || r.HasMintMethod || r.HasBlackMethod || r.IsProxy
}

type Input struct {
	ChainID          uint64
	WalletAddress    string
	Trades           []Trade
	Prices           map[string]Price
	Risks            map[string]Risk
	TransferInCount  uint64
	TransferOutCount uint64
}

type TokenPnL struct {
	ChainID                uint64
	WalletAddress          string
	TokenAddress           string
	TokenSymbol            string
	AnalyticsVersion       string
	IsQuoteToken           bool
	BoughtAmountRaw        string
	SoldAmountRaw          string
	RemainingAmountRaw     string
	TotalBuyCostUSDRaw     string
	TotalSellIncomeUSDRaw  string
	RemainingCostUSDRaw    string
	RealizedProfitUSDRaw   string
	UnrealizedProfitUSDRaw string
	TotalProfitUSDRaw      string
	CurrentValueUSDRaw     string
	AverageCostUSDRaw      string
	CurrentPriceUSDRaw     string
	BuyCount               uint64
	SellCount              uint64
	WinningSellCount       uint64
	UnmatchedSellAmountRaw string
	UnmatchedSellUSDRaw    string
	Risk                   Risk
	DataQuality            string
	FirstTradedAt          time.Time
	LastTradedAt           time.Time
	PriceUpdatedAt         time.Time
	SourceUpdatedAt        time.Time
	CalculatedAt           time.Time
}

type WalletScore struct {
	ChainID                   uint64
	WalletAddress             string
	AnalyticsVersion          string
	RealizedProfitUSDRaw      string
	UnrealizedProfitUSDRaw    string
	TotalProfitUSDRaw         string
	TotalInvestedUSDRaw       string
	ROIRaw                    string
	WinRateRaw                string
	SmartScoreRaw             string
	SmartScoreGrade           string
	PerformanceScoreRaw       string
	WinRateScoreRaw           string
	TrackRecordScoreRaw       string
	ActivityScoreRaw          string
	RiskScoreRaw              string
	ConfidenceRaw             string
	TradeCount                uint64
	ClosedSellCount           uint64
	WinningSellCount          uint64
	ActiveDays                uint64
	UniqueNonQuoteTokens      uint64
	RiskyTokenCount           uint64
	UnmatchedSellCount        uint64
	MissingPricePositionCount uint64
	PartialValuationCount     uint64
	TransferInCount           uint64
	TransferOutCount          uint64
	HistoryIncomplete         bool
	SourceUpdatedAt           time.Time
	SourceUpdatedAtMS         uint64
	CalculatedAt              time.Time
}

type Result struct {
	Tokens []TokenPnL
	Score  WalletScore
}

type Store interface {
	WalletAnalysisCandidates(
		ctx context.Context,
		analyticsVersion string,
		limit int,
	) ([]Candidate, error)
	LoadWalletAnalysis(ctx context.Context, candidate Candidate) (Input, error)
	InsertWalletAnalysis(ctx context.Context, result Result) error
}
