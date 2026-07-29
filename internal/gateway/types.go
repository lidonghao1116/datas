package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var ErrNotFound = errors.New("not found")

type Alert struct {
	Key             string          `json:"alert_key"`
	Type            string          `json:"alert_type"`
	Severity        string          `json:"severity"`
	Status          string          `json:"status"`
	ChainID         uint64          `json:"chain_id"`
	BlockNumber     uint64          `json:"block_number"`
	TransactionHash string          `json:"transaction_hash"`
	TokenAddress    string          `json:"token_address"`
	TokenSymbol     string          `json:"token_symbol"`
	Title           string          `json:"title"`
	Payload         json.RawMessage `json:"payload"`
	Attempts        int             `json:"attempts"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	DeliveredAt     *time.Time      `json:"delivered_at,omitempty"`
}

type AlertCursor struct {
	CreatedAt time.Time
	Key       string
}

type AlertFilter struct {
	Status   string
	Severity string
	Type     string
	Limit    int
}

type LargeTrade struct {
	EventID            string    `json:"event_id"`
	BlockNumber        uint64    `json:"block_number"`
	BlockTime          time.Time `json:"block_time"`
	TransactionHash    string    `json:"transaction_hash"`
	PoolAddress        string    `json:"pool_address"`
	Protocol           string    `json:"protocol"`
	ProtocolVersion    string    `json:"protocol_version"`
	BoughtTokenAddress string    `json:"bought_token_address"`
	BoughtTokenSymbol  string    `json:"bought_token_symbol"`
	SoldTokenAddress   string    `json:"sold_token_address"`
	SoldTokenSymbol    string    `json:"sold_token_symbol"`
	TradeValueUSDRaw   string    `json:"trade_value_usd_raw"`
	ValuationStatus    string    `json:"valuation_status"`
	ValuedAt           time.Time `json:"valued_at"`
}

type TokenMarket struct {
	ChainID           uint64     `json:"chain_id"`
	TokenAddress      string     `json:"token_address"`
	Source            string     `json:"source"`
	PriceUSDRaw       string     `json:"price_usd_raw"`
	PriceChange24hRaw string     `json:"price_change_24h_raw"`
	TVLUSDRaw         string     `json:"tvl_usd_raw"`
	MarketCapUSDRaw   string     `json:"market_cap_usd_raw"`
	FDVUSDRaw         string     `json:"fdv_usd_raw"`
	Volume24hUSDRaw   string     `json:"volume_24h_usd_raw"`
	Holders           uint64     `json:"holders"`
	MarketUpdatedAt   time.Time  `json:"market_updated_at"`
	RiskScoreRaw      string     `json:"risk_score_raw"`
	IsHoneypot        *uint8     `json:"is_honeypot"`
	HasMintMethod     *uint8     `json:"has_mint_method"`
	HasBlackMethod    *uint8     `json:"has_black_method"`
	IsProxy           *uint8     `json:"is_proxy"`
	RiskUpdatedAt     *time.Time `json:"risk_updated_at,omitempty"`
}

type WalletProfile struct {
	ChainID              uint64             `json:"chain_id"`
	WalletAddress        string             `json:"wallet_address"`
	AttributionMethod    string             `json:"attribution_method"`
	FirstActiveAt        time.Time          `json:"first_active_at"`
	LastActiveAt         time.Time          `json:"last_active_at"`
	ActiveDays           uint64             `json:"active_days"`
	TransactionCount     uint64             `json:"transaction_count"`
	SwapTransactionCount uint64             `json:"swap_transaction_count"`
	SwapCount            uint64             `json:"swap_count"`
	BuyCount             uint64             `json:"buy_count"`
	SellCount            uint64             `json:"sell_count"`
	OtherSwapCount       uint64             `json:"other_swap_count"`
	TransferInCount      uint64             `json:"transfer_in_count"`
	TransferOutCount     uint64             `json:"transfer_out_count"`
	SwapVolumeUSDRaw     string             `json:"swap_volume_usd_raw"`
	UniqueSwapTokens     uint64             `json:"unique_swap_tokens"`
	FavoriteTokenAddress string             `json:"favorite_token_address"`
	FavoriteTokenSymbol  string             `json:"favorite_token_symbol"`
	FavoriteProtocol     string             `json:"favorite_protocol"`
	ProfileUpdatedAt     time.Time          `json:"profile_updated_at"`
	GMGN                 *GMGNWalletProfile `json:"gmgn,omitempty"`
}

type GMGNWalletProfile struct {
	Source    string                           `json:"source"`
	Available bool                             `json:"available"`
	Identity  GMGNWalletIdentity               `json:"identity"`
	Periods   map[string]GMGNWalletPeriodStats `json:"periods"`
	Sync      map[string]WalletEnrichmentSync  `json:"sync"`
}

type GMGNWalletIdentity struct {
	DisplayName       string     `json:"display_name"`
	ENS               string     `json:"ens"`
	PrimaryTag        string     `json:"primary_tag"`
	Tags              []string   `json:"tags"`
	TwitterUsername   string     `json:"twitter_username"`
	TwitterName       string     `json:"twitter_name"`
	TwitterFollowers  uint64     `json:"twitter_followers"`
	IsBlueVerified    bool       `json:"is_blue_verified"`
	CreatedTokenCount uint64     `json:"created_token_count"`
	WalletCreatedAt   *time.Time `json:"wallet_created_at,omitempty"`
	FundFrom          string     `json:"fund_from"`
	FundFromAddress   string     `json:"fund_from_address"`
	FundAmountRaw     string     `json:"fund_amount_raw"`
}

type GMGNWalletPeriodStats struct {
	Period              string     `json:"period"`
	NativeBalanceRaw    string     `json:"native_balance_raw"`
	RealizedProfitRaw   string     `json:"realized_profit_raw"`
	UnrealizedProfitRaw string     `json:"unrealized_profit_raw"`
	PnLRaw              string     `json:"pnl_raw"`
	WinRateRaw          string     `json:"win_rate_raw"`
	TotalCostRaw        string     `json:"total_cost_raw"`
	BuyCount            uint64     `json:"buy_count"`
	SellCount           uint64     `json:"sell_count"`
	TokenCount          uint64     `json:"token_count"`
	AvgHoldingSeconds   uint64     `json:"avg_holding_seconds"`
	SourceUpdatedAt     *time.Time `json:"source_updated_at,omitempty"`
	FetchedAt           time.Time  `json:"fetched_at"`
	ExpiresAt           time.Time  `json:"expires_at"`
	IsStale             bool       `json:"is_stale"`
}

type WalletEnrichmentSync struct {
	Status       string    `json:"status"`
	AttemptCount uint32    `json:"attempt_count"`
	AttemptedAt  time.Time `json:"attempted_at"`
	NextRetryAt  time.Time `json:"next_retry_at"`
}

type WalletTrade struct {
	EventID              string    `json:"event_id"`
	ChainID              uint64    `json:"chain_id"`
	WalletAddress        string    `json:"wallet_address"`
	RouterAddress        string    `json:"router_address"`
	AttributionMethod    string    `json:"attribution_method"`
	BlockNumber          uint64    `json:"block_number"`
	BlockTime            time.Time `json:"block_time"`
	TransactionHash      string    `json:"transaction_hash"`
	PoolAddress          string    `json:"pool_address"`
	Protocol             string    `json:"protocol"`
	ProtocolVersion      string    `json:"protocol_version"`
	BoughtTokenAddress   string    `json:"bought_token_address"`
	BoughtTokenSymbol    string    `json:"bought_token_symbol"`
	BoughtTokenAmountRaw string    `json:"bought_token_amount_raw"`
	SoldTokenAddress     string    `json:"sold_token_address"`
	SoldTokenSymbol      string    `json:"sold_token_symbol"`
	SoldTokenAmountRaw   string    `json:"sold_token_amount_raw"`
	TradeValueUSDRaw     string    `json:"trade_value_usd_raw"`
	ValuationStatus      string    `json:"valuation_status"`
	SourceValuedAt       time.Time `json:"source_valued_at"`
}

type WalletPosition struct {
	ChainID          uint64    `json:"chain_id"`
	WalletAddress    string    `json:"wallet_address"`
	TokenAddress     string    `json:"token_address"`
	TokenSymbol      string    `json:"token_symbol"`
	BoughtAmountRaw  string    `json:"bought_amount_raw"`
	SoldAmountRaw    string    `json:"sold_amount_raw"`
	NetAmountRaw     string    `json:"net_amount_raw"`
	BuyCount         uint64    `json:"buy_count"`
	SellCount        uint64    `json:"sell_count"`
	SwapVolumeUSDRaw string    `json:"swap_volume_usd_raw"`
	FirstTradedAt    time.Time `json:"first_traded_at"`
	LastTradedAt     time.Time `json:"last_traded_at"`
	PositionBasis    string    `json:"position_basis"`
}

type AlertStore interface {
	RecentAlerts(ctx context.Context, filter AlertFilter) ([]Alert, error)
	AlertsAfter(
		ctx context.Context,
		cursor AlertCursor,
		limit int,
	) ([]Alert, error)
	Ping(ctx context.Context) error
}

type AnalyticsStore interface {
	RecentLargeTrades(ctx context.Context, limit int) ([]LargeTrade, error)
	TokenMarket(ctx context.Context, chainID uint64, address string) (TokenMarket, error)
	WalletProfile(ctx context.Context, chainID uint64, address string) (WalletProfile, error)
	WalletTrades(
		ctx context.Context,
		chainID uint64,
		address string,
		limit int,
	) ([]WalletTrade, error)
	WalletPositions(
		ctx context.Context,
		chainID uint64,
		address string,
		limit int,
	) ([]WalletPosition, error)
	Ping(ctx context.Context) error
}
