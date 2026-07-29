package base

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetchBlockFallsBackToTransactionReceipts(t *testing.T) {
	var receiptCalls atomic.Int32
	var maxBatchSize atomic.Int32
	transactions := make([]any, 11)
	for index := range transactions {
		transactions[index] = map[string]any{"hash": testTransactionHash}
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		var payload json.RawMessage
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}

		if len(payload) > 0 && payload[0] == '[' {
			var requests []rpcRequest
			if err := json.Unmarshal(payload, &requests); err != nil {
				t.Errorf("decode batch: %v", err)
				return
			}
			if len(requests) > 10 {
				_ = json.NewEncoder(writer).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      nil,
					"error": map[string]any{
						"code":    -32014,
						"message": "maximum 10 calls in 1 batch",
					},
				})
				return
			}
			if size := int32(len(requests)); size > maxBatchSize.Load() {
				maxBatchSize.Store(size)
			}
			responses := make([]any, 0, len(requests))
			for _, rpcRequest := range requests {
				receiptCalls.Add(1)
				responses = append(responses, map[string]any{
					"jsonrpc": "2.0",
					"id":      rpcRequest.ID,
					"result": map[string]any{
						"transactionHash":  testTransactionHash,
						"transactionIndex": "0x0",
						"status":           "0x1",
						"gasUsed":          "0x5208",
						"logs":             []any{},
					},
				})
			}
			_ = json.NewEncoder(writer).Encode(responses)
			return
		}

		var rpcRequest rpcRequest
		if err := json.Unmarshal(payload, &rpcRequest); err != nil {
			t.Errorf("decode RPC request: %v", err)
			return
		}
		switch rpcRequest.Method {
		case "eth_getBlockByNumber":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      rpcRequest.ID,
				"result": map[string]any{
					"number":       "0x64",
					"hash":         testBlockHash,
					"parentHash":   testParentHash,
					"timestamp":    "0x1",
					"transactions": transactions,
				},
			})
		case "eth_getBlockReceipts":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      rpcRequest.ID,
				"error": map[string]any{
					"code":    -32601,
					"message": "rpc method is unsupported",
				},
			})
		default:
			t.Errorf("unexpected method %s", rpcRequest.Method)
		}
	}))
	defer server.Close()

	// The timeout applies to each RPC request, not to the complete block. The
	// fallback intentionally pauses between its two receipt batches for longer
	// than this timeout.
	client := NewClient(server.URL, "", 8453, 100*time.Millisecond)
	envelope, err := client.FetchBlock(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(envelope.Receipts) != len(transactions) {
		t.Fatalf("expected %d receipts, got %d", len(transactions), len(envelope.Receipts))
	}
	if receiptCalls.Load() != int32(len(transactions)) {
		t.Fatalf("expected %d transaction receipt calls, got %d", len(transactions), receiptCalls.Load())
	}
	if maxBatchSize.Load() > 10 {
		t.Fatalf("expected batches of at most 10 calls, got %d", maxBatchSize.Load())
	}
}

func TestRateLimited(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     error
		limited bool
	}{
		{name: "nil", err: nil, limited: false},
		{name: "provider wording", err: errors.New("over rate limit"), limited: true},
		{name: "http wording", err: errors.New("too many requests"), limited: true},
		{name: "other RPC error", err: errors.New("method not found"), limited: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if actual := rateLimited(test.err); actual != test.limited {
				t.Fatalf("expected %t, got %t", test.limited, actual)
			}
		})
	}
}

type rpcRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
}

const (
	testBlockHash       = "0x0000000000000000000000000000000000000000000000000000000000000001"
	testParentHash      = "0x0000000000000000000000000000000000000000000000000000000000000002"
	testTransactionHash = "0x0000000000000000000000000000000000000000000000000000000000000003"
)
