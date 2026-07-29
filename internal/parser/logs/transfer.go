package logs

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/basewatch/base-analytics/internal/domain"
)

var transferTopic = strings.ToLower(
	crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)")).Hex(),
)

func decodeTransfer(meta domain.EventMeta, log RawLog) (domain.ERC20Transfer, error) {
	if len(log.Topics) != 3 {
		return domain.ERC20Transfer{}, fmt.Errorf("ERC-20 Transfer requires 3 topics, got %d", len(log.Topics))
	}
	words, err := decodeWords(log.Data, 1)
	if err != nil {
		return domain.ERC20Transfer{}, err
	}
	from, err := indexedAddress(log.Topics[1])
	if err != nil {
		return domain.ERC20Transfer{}, err
	}
	to, err := indexedAddress(log.Topics[2])
	if err != nil {
		return domain.ERC20Transfer{}, err
	}
	return domain.ERC20Transfer{
		EventMeta:    meta,
		TokenAddress: strings.ToLower(log.Address),
		FromAddress:  from,
		ToAddress:    to,
		AmountRaw:    unsignedWord(words[0]).String(),
	}, nil
}
