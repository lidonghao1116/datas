package logs

import (
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/basewatch/base-analytics/internal/domain"
)

func TestParserDecodesTransferAndV2V3Swaps(t *testing.T) {
	from := "0x1111111111111111111111111111111111111111"
	to := "0x2222222222222222222222222222222222222222"
	poolV2 := "0x3333333333333333333333333333333333333333"
	poolV3 := "0x4444444444444444444444444444444444444444"
	transactionHash := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	receipt, err := json.Marshal(map[string]any{
		"logs": []any{
			map[string]any{
				"address":          "0x5555555555555555555555555555555555555555",
				"topics":           []string{transferTopic, addressTopic(from), addressTopic(to)},
				"data":             encodeWords(big.NewInt(42)),
				"transactionHash":  transactionHash,
				"transactionIndex": "0x0",
				"logIndex":         "0x0",
				"removed":          false,
			},
			map[string]any{
				"address":          poolV2,
				"topics":           []string{v2SwapTopic, addressTopic(from), addressTopic(to)},
				"data":             encodeWords(big.NewInt(100), big.NewInt(0), big.NewInt(0), big.NewInt(50)),
				"transactionHash":  transactionHash,
				"transactionIndex": "0x0",
				"logIndex":         "0x1",
				"removed":          false,
			},
			map[string]any{
				"address":          poolV3,
				"topics":           []string{v3SwapTopic, addressTopic(from), addressTopic(to)},
				"data":             encodeWords(big.NewInt(-7), big.NewInt(9), big.NewInt(123), big.NewInt(456), big.NewInt(-10)),
				"transactionHash":  transactionHash,
				"transactionIndex": "0x0",
				"logIndex":         "0x2",
				"removed":          false,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	envelope := domain.RawBlockEnvelope{
		SchemaVersion: domain.RawBlockSchemaVersion,
		ChainID:       8453,
		BlockNumber:   100,
		BlockHash:     "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ParentHash:    "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		BlockTime:     time.Unix(1, 0).UTC(),
		ObservedAt:    time.Unix(2, 0).UTC(),
		Block:         json.RawMessage(`{"transactions":[]}`),
		Receipts:      []json.RawMessage{receipt},
	}

	result, err := NewParser().Parse(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Transfers) != 1 {
		t.Fatalf("expected 1 transfer, got %d", len(result.Transfers))
	}
	if result.Transfers[0].AmountRaw != "42" {
		t.Fatalf("expected transfer amount 42, got %s", result.Transfers[0].AmountRaw)
	}
	if result.Transfers[0].FromAddress != strings.ToLower(from) {
		t.Fatalf("unexpected transfer sender %s", result.Transfers[0].FromAddress)
	}

	if len(result.Swaps) != 2 {
		t.Fatalf("expected 2 swaps, got %d", len(result.Swaps))
	}
	if result.Swaps[0].ProtocolFamily != "uniswap_v2_compatible" {
		t.Fatalf("unexpected V2 protocol family %s", result.Swaps[0].ProtocolFamily)
	}
	if result.Swaps[0].Amount0DeltaRaw != "100" || result.Swaps[0].Amount1DeltaRaw != "-50" {
		t.Fatalf(
			"unexpected V2 deltas %s/%s",
			result.Swaps[0].Amount0DeltaRaw,
			result.Swaps[0].Amount1DeltaRaw,
		)
	}
	if result.Swaps[1].ProtocolFamily != "uniswap_v3_compatible" {
		t.Fatalf("unexpected V3 protocol family %s", result.Swaps[1].ProtocolFamily)
	}
	if result.Swaps[1].Amount0DeltaRaw != "-7" || result.Swaps[1].Amount1DeltaRaw != "9" {
		t.Fatalf(
			"unexpected V3 deltas %s/%s",
			result.Swaps[1].Amount0DeltaRaw,
			result.Swaps[1].Amount1DeltaRaw,
		)
	}
	if result.Swaps[1].Tick != -10 {
		t.Fatalf("expected tick -10, got %d", result.Swaps[1].Tick)
	}
}

func TestParserSkipsRemovedLogs(t *testing.T) {
	receipt := json.RawMessage(`{
		"logs":[{
			"address":"0x5555555555555555555555555555555555555555",
			"topics":["` + transferTopic + `","` + addressTopic("0x1111111111111111111111111111111111111111") + `","` + addressTopic("0x2222222222222222222222222222222222222222") + `"],
			"data":"` + encodeWords(big.NewInt(1)) + `",
			"transactionHash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"transactionIndex":"0x0",
			"logIndex":"0x0",
			"removed":true
		}]
	}`)
	envelope := domain.RawBlockEnvelope{
		SchemaVersion: domain.RawBlockSchemaVersion,
		ChainID:       8453,
		BlockHash:     "0xbb",
		ParentHash:    "0xcc",
		Block:         json.RawMessage(`{}`),
		Receipts:      []json.RawMessage{receipt},
	}
	result, err := NewParser().Parse(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Transfers) != 0 || len(result.Swaps) != 0 {
		t.Fatal("removed log should not produce normalized events")
	}
}

func TestParserDoesNotTreatERC721AsERC20(t *testing.T) {
	receipt, err := json.Marshal(map[string]any{
		"logs": []any{
			map[string]any{
				"address": "0x5555555555555555555555555555555555555555",
				"topics": []string{
					transferTopic,
					addressTopic("0x1111111111111111111111111111111111111111"),
					addressTopic("0x2222222222222222222222222222222222222222"),
					encodeWords(big.NewInt(123)),
				},
				"data":             "0x",
				"transactionHash":  "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"transactionIndex": "0x0",
				"logIndex":         "0x0",
				"removed":          false,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope := domain.RawBlockEnvelope{
		SchemaVersion: domain.RawBlockSchemaVersion,
		ChainID:       8453,
		BlockHash:     "0xbb",
		ParentHash:    "0xcc",
		Block:         json.RawMessage(`{}`),
		Receipts:      []json.RawMessage{receipt},
	}
	result, err := NewParser().Parse(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Transfers) != 0 {
		t.Fatal("ERC-721 Transfer must not be stored as ERC-20 Transfer")
	}
}

func addressTopic(address string) string {
	return hexutil.Encode(common.LeftPadBytes(common.HexToAddress(address).Bytes(), 32))
}

func encodeWords(values ...*big.Int) string {
	var encoded []byte
	for _, value := range values {
		normalized := new(big.Int).Set(value)
		if normalized.Sign() < 0 {
			normalized.Add(normalized, twoTo256)
		}
		encoded = append(encoded, common.LeftPadBytes(normalized.Bytes(), 32)...)
	}
	return "0x" + hex.EncodeToString(encoded)
}
