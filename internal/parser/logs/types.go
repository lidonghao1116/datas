package logs

import (
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/basewatch/base-analytics/internal/domain"
)

const ParserVersion = "logs-v1"

type RawLog struct {
	Address          string         `json:"address"`
	Topics           []string       `json:"topics"`
	Data             string         `json:"data"`
	TransactionHash  string         `json:"transactionHash"`
	TransactionIndex hexutil.Uint64 `json:"transactionIndex"`
	LogIndex         hexutil.Uint64 `json:"logIndex"`
	Removed          bool           `json:"removed"`
}

type Result struct {
	Transfers []domain.ERC20Transfer
	Swaps     []domain.PoolSwap
}

type SwapDecoder interface {
	Topic0() string
	Decode(meta domain.EventMeta, log RawLog) (domain.PoolSwap, error)
}

func metaFrom(envelope domain.RawBlockEnvelope, log RawLog) domain.EventMeta {
	return domain.EventMeta{
		SchemaVersion:    domain.NormalizedEventSchemaVersion,
		ChainID:          envelope.ChainID,
		BlockNumber:      envelope.BlockNumber,
		BlockHash:        envelope.BlockHash,
		BlockTime:        envelope.BlockTime,
		TransactionHash:  log.TransactionHash,
		TransactionIndex: uint32(log.TransactionIndex),
		LogIndex:         uint32(log.LogIndex),
		ObservedAt:       envelope.ObservedAt,
		IsCanonical:      1,
		ParserVersion:    ParserVersion,
	}
}

func parseReceiptLogs(raw json.RawMessage) ([]RawLog, error) {
	var receipt struct {
		Logs []RawLog `json:"logs"`
	}
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return nil, fmt.Errorf("decode receipt logs: %w", err)
	}
	return receipt.Logs, nil
}
