package marketdata

import (
	"context"
	"encoding/json"
	"time"
)

type Token struct {
	ChainID uint64
	Address string
}

type MarketSnapshot struct {
	Token
	Source            string
	PriceUSDRaw       string
	PriceChange24hRaw string
	TVLUSDRaw         string
	MarketCapUSDRaw   string
	FDVUSDRaw         string
	Volume24hUSDRaw   string
	Holders           uint64
	SourceUpdatedAt   time.Time
	FetchedAt         time.Time
}

type RiskSnapshot struct {
	Token
	Source          string
	RiskScoreRaw    string
	IsHoneypot      *uint8
	HasMintMethod   *uint8
	HasBlackMethod  *uint8
	IsProxy         *uint8
	OwnerAddress    string
	BuyTaxRaw       string
	SellTaxRaw      string
	RawJSON         json.RawMessage
	SourceUpdatedAt time.Time
	FetchedAt       time.Time
}

type Provider interface {
	MarketSnapshots(ctx context.Context, tokens []Token) ([]MarketSnapshot, error)
	RiskSnapshot(ctx context.Context, token Token) (RiskSnapshot, error)
}
