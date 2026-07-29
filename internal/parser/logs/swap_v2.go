package logs

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/basewatch/base-analytics/internal/domain"
)

type V2SwapDecoder struct{}

var v2SwapTopic = strings.ToLower(
	crypto.Keccak256Hash(
		[]byte("Swap(address,uint256,uint256,uint256,uint256,address)"),
	).Hex(),
)

func (V2SwapDecoder) Topic0() string {
	return v2SwapTopic
}

func (V2SwapDecoder) Decode(meta domain.EventMeta, log RawLog) (domain.PoolSwap, error) {
	if len(log.Topics) != 3 {
		return domain.PoolSwap{}, fmt.Errorf("V2 Swap requires 3 topics, got %d", len(log.Topics))
	}
	words, err := decodeWords(log.Data, 4)
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

	amount0In := unsignedWord(words[0])
	amount1In := unsignedWord(words[1])
	amount0Out := unsignedWord(words[2])
	amount1Out := unsignedWord(words[3])
	amount0Delta := new(big.Int).Sub(amount0In, amount0Out)
	amount1Delta := new(big.Int).Sub(amount1In, amount1Out)

	return domain.PoolSwap{
		EventMeta:        meta,
		PoolAddress:      strings.ToLower(log.Address),
		ProtocolFamily:   "uniswap_v2_compatible",
		SenderAddress:    sender,
		RecipientAddress: recipient,
		Amount0DeltaRaw:  amount0Delta.String(),
		Amount1DeltaRaw:  amount1Delta.String(),
		SqrtPriceX96Raw:  "0",
		LiquidityRaw:     "0",
	}, nil
}
