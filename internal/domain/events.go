package domain

import (
	"fmt"
	"time"
)

const NormalizedEventSchemaVersion = "2"

type EventMeta struct {
	SchemaVersion    string    `json:"schema_version"`
	ChainID          uint64    `json:"chain_id"`
	BlockNumber      uint64    `json:"block_number"`
	BlockHash        string    `json:"block_hash"`
	BlockTime        time.Time `json:"block_time"`
	TransactionHash  string    `json:"transaction_hash"`
	TransactionIndex uint32    `json:"transaction_index"`
	LogIndex         uint32    `json:"log_index"`
	ObservedAt       time.Time `json:"observed_at"`
	IsCanonical      uint8     `json:"is_canonical"`
	ParserVersion    string    `json:"parser_version"`
}

func (m EventMeta) EventID() string {
	return eventID(m.ChainID, m.BlockHash, m.TransactionHash, m.LogIndex)
}

type ERC20Transfer struct {
	EventMeta
	TokenAddress string `json:"token_address"`
	FromAddress  string `json:"from_address"`
	ToAddress    string `json:"to_address"`
	AmountRaw    string `json:"amount_raw"`
}

type PoolSwap struct {
	EventMeta
	PoolAddress      string `json:"pool_address"`
	FactoryAddress   string `json:"factory_address"`
	Protocol         string `json:"protocol"`
	ProtocolVersion  string `json:"protocol_version"`
	ProtocolFamily   string `json:"protocol_family"`
	Token0Address    string `json:"token0_address"`
	Token1Address    string `json:"token1_address"`
	Token0Symbol     string `json:"token0_symbol"`
	Token1Symbol     string `json:"token1_symbol"`
	Token0Decimals   uint8  `json:"token0_decimals"`
	Token1Decimals   uint8  `json:"token1_decimals"`
	MetadataStatus   string `json:"metadata_status"`
	SenderAddress    string `json:"sender_address"`
	RecipientAddress string `json:"recipient_address"`
	Amount0DeltaRaw  string `json:"amount0_delta_raw"`
	Amount1DeltaRaw  string `json:"amount1_delta_raw"`
	SqrtPriceX96Raw  string `json:"sqrt_price_x96_raw"`
	LiquidityRaw     string `json:"liquidity_raw"`
	Tick             int32  `json:"tick"`
}

func eventID(chainID uint64, blockHash, transactionHash string, logIndex uint32) string {
	return fmt.Sprintf("%d:%s:%s:%d", chainID, blockHash, transactionHash, logIndex)
}
