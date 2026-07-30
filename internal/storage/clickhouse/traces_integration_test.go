package clickhouse

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/basewatch/base-analytics/internal/traceanalytics"
)

func TestTracePersistenceIntegration(t *testing.T) {
	if os.Getenv("TRACE_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set TRACE_CLICKHOUSE_INTEGRATION=1 to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := OpenEventStore(
		ctx,
		envOr("TRACE_CLICKHOUSE_ADDR", "host.docker.internal:9000"),
		"base",
		"default",
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const transactionHash = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff14"
	defer deleteSyntheticTrace(t, store, transactionHash)
	now := time.Now().UTC()
	result := traceanalytics.Result{
		Candidate: traceanalytics.Candidate{
			ChainID:          8453,
			BlockNumber:      1,
			BlockHash:        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			BlockTime:        now,
			TransactionHash:  transactionHash,
			TransactionIndex: 1,
			WalletAddress:    "0x1111111111111111111111111111111111111111",
			TargetAddress:    "0x2222222222222222222222222222222222222222",
		},
		Calls: []traceanalytics.Call{{
			TraceID:          transactionHash + ":root",
			CallType:         "CALL",
			FromAddress:      "0x1111111111111111111111111111111111111111",
			ToAddress:        "0x2222222222222222222222222222222222222222",
			ValueRaw:         "0x0",
			GasRaw:           "0x100",
			GasUsedRaw:       "0x80",
			Input:            "0x3593564c",
			FunctionSelector: "0x3593564c",
			FunctionName:     "execute(bytes,bytes[])",
			Success:          true,
			IsRouterCall:     true,
			IsMulticall:      true,
		}},
		RootSelector:       "0x3593564c",
		RootFunction:       "execute(bytes,bytes[])",
		FrameCount:         1,
		RouterAddresses:    []string{"0x2222222222222222222222222222222222222222"},
		MulticallSelectors: []string{"0x3593564c"},
		RawTrace:           []byte(`{"type":"CALL"}`),
		TracedAt:           now,
	}
	if err := store.InsertTrace(ctx, result); err != nil {
		t.Fatal(err)
	}
	stored, err := store.TransactionTrace(ctx, 8453, transactionHash)
	if err != nil {
		t.Fatal(err)
	}
	if stored.FrameCount != 1 || len(stored.Calls) != 1 ||
		stored.Calls[0].FunctionName != "execute(bytes,bytes[])" {
		t.Fatalf("unexpected stored trace: %+v", stored)
	}
}

func deleteSyntheticTrace(t *testing.T, store *EventStore, transactionHash string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, table := range []string{
		"transaction_call_traces",
		"transaction_trace_summaries",
		"transaction_trace_sync_state",
	} {
		if err := store.conn.Exec(
			ctx,
			"ALTER TABLE "+table+
				" DELETE WHERE transaction_hash = ? SETTINGS mutations_sync = 2",
			transactionHash,
		); err != nil {
			t.Errorf("clean synthetic trace from %s: %v", table, err)
		}
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
