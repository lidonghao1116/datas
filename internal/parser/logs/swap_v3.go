package logs

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/basewatch/base-analytics/internal/domain"
)

type V3SwapDecoder struct{}

var v3SwapTopic = strings.ToLower(
	crypto.Keccak256Hash(
		[]byte("Swap(address,address,int256,int256,uint160,uint128,int24)"),
	).Hex(),
)

func (V3SwapDecoder) Topic0() string {
	return v3SwapTopic
}

func (V3SwapDecoder) Decode(meta domain.EventMeta, log RawLog) (domain.PoolSwap, error) {
	if len(log.Topics) != 3 {
		return domain.PoolSwap{}, fmt.Errorf("V3 Swap requires 3 topics, got %d", len(log.Topics))
	}
	words, err := decodeWords(log.Data, 5)
	if err != nil {
		return domain.PoolSwap{}, err
	}
	sender, err := indexedAddress(log.Topics[1])
	if err != nil {
		return domain.PoolSwap{}, err
	}
	recipient, err := indexedAddress(log.Topics[2])
	if err != nil {
		return domain.PoolSwap{}, err
	}
	tick := signedWord(words[4])
	if !tick.IsInt64() {
		return domain.PoolSwap{}, fmt.Errorf("V3 tick is outside int64 range")
	}

	return domain.PoolSwap{
		EventMeta:        meta,
		PoolAddress:      strings.ToLower(log.Address),
		ProtocolFamily:   "uniswap_v3_compatible",
		SenderAddress:    sender,
		RecipientAddress: recipient,
		Amount0DeltaRaw:  signedWord(words[0]).String(),
		Amount1DeltaRaw:  signedWord(words[1]).String(),
		SqrtPriceX96Raw:  unsignedWord(words[2]).String(),
		LiquidityRaw:     unsignedWord(words[3]).String(),
		Tick:             int32(tick.Int64()),
	}, nil
}
