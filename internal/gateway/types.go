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
	Ping(ctx context.Context) error
}
