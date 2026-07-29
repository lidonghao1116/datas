package logs

import (
	"fmt"
	"strings"

	"github.com/basewatch/base-analytics/internal/domain"
)

type Parser struct {
	swapDecoders map[string]SwapDecoder
}

func NewParser(decoders ...SwapDecoder) *Parser {
	if len(decoders) == 0 {
		decoders = []SwapDecoder{V2SwapDecoder{}, V3SwapDecoder{}}
	}
	registry := make(map[string]SwapDecoder, len(decoders))
	for _, decoder := range decoders {
		registry[strings.ToLower(decoder.Topic0())] = decoder
	}
	return &Parser{swapDecoders: registry}
}

func (p *Parser) Parse(envelope domain.RawBlockEnvelope) (Result, error) {
	if err := envelope.Validate(); err != nil {
		return Result{}, err
	}
	var result Result
	for receiptIndex, rawReceipt := range envelope.Receipts {
		rawLogs, err := parseReceiptLogs(rawReceipt)
		if err != nil {
			return Result{}, fmt.Errorf("receipt %d: %w", receiptIndex, err)
		}
		for _, rawLog := range rawLogs {
			if rawLog.Removed || len(rawLog.Topics) == 0 {
				continue
			}
			topic0 := strings.ToLower(rawLog.Topics[0])
			meta := metaFrom(envelope, rawLog)

			if topic0 == transferTopic {
				// ERC-721 uses the same event signature but has an indexed tokenId,
				// producing four topics. Only the canonical ERC-20 layout has
				// topic0/from/to plus one 32-byte value in data.
				if len(rawLog.Topics) != 3 {
					continue
				}
				transfer, err := decodeTransfer(meta, rawLog)
				if err != nil {
					return Result{}, fmt.Errorf("decode Transfer at log %d: %w", rawLog.LogIndex, err)
				}
				result.Transfers = append(result.Transfers, transfer)
				continue
			}
			if decoder, exists := p.swapDecoders[topic0]; exists {
				swap, err := decoder.Decode(meta, rawLog)
				if err != nil {
					return Result{}, fmt.Errorf("decode Swap at log %d: %w", rawLog.LogIndex, err)
				}
				result.Swaps = append(result.Swaps, swap)
			}
		}
	}
	return result, nil
}
