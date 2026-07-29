package logs

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var twoTo256 = new(big.Int).Lsh(big.NewInt(1), 256)

func decodeWords(data string, count int) ([][]byte, error) {
	decoded, err := hexutil.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("decode event data: %w", err)
	}
	if len(decoded) != count*32 {
		return nil, fmt.Errorf("expected %d data bytes, got %d", count*32, len(decoded))
	}
	words := make([][]byte, count)
	for index := 0; index < count; index++ {
		words[index] = decoded[index*32 : (index+1)*32]
	}
	return words, nil
}

func unsignedWord(word []byte) *big.Int {
	return new(big.Int).SetBytes(word)
}

func signedWord(word []byte) *big.Int {
	value := new(big.Int).SetBytes(word)
	if len(word) == 32 && word[0]&0x80 != 0 {
		value.Sub(value, twoTo256)
	}
	return value
}

func indexedAddress(topic string) (string, error) {
	if len(strings.TrimPrefix(topic, "0x")) != 64 {
		return "", fmt.Errorf("invalid indexed address topic %q", topic)
	}
	return strings.ToLower(common.HexToAddress(topic).Hex()), nil
}
