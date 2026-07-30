package traceanalytics

import (
	"testing"
	"time"
)

func TestAnalyzeMarksRouterMulticallDelegatecallAndPool(t *testing.T) {
	candidate := testCandidate()
	root := CallFrame{
		Type:  "CALL",
		From:  candidate.WalletAddress,
		To:    candidate.TargetAddress,
		Input: "0x3593564c00",
		Calls: []CallFrame{{
			Type:  "DELEGATECALL",
			From:  candidate.TargetAddress,
			To:    "0x3333333333333333333333333333333333333333",
			Input: "0x414bf38900",
			Calls: []CallFrame{{
				Type:    "CALL",
				From:    "0x3333333333333333333333333333333333333333",
				To:      candidate.PoolAddresses[0],
				Input:   "0x128acb0800",
				GasUsed: "0x100",
				Logs:    []CallLog{{Address: candidate.PoolAddresses[0]}},
			}},
		}, {
			Type:  "STATICCALL",
			From:  candidate.TargetAddress,
			To:    "0x4444444444444444444444444444444444444444",
			Input: "0x70a0823100",
			Error: "execution reverted",
		}},
	}
	result, err := Analyze(candidate, root, []byte(`{"type":"CALL"}`), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.FrameCount != 4 || result.MaxDepth != 2 {
		t.Fatalf("unexpected frame summary: %+v", result)
	}
	if result.PoolCallCount != 1 || result.DelegatecallCount != 1 ||
		result.FailedCallCount != 1 {
		t.Fatalf("unexpected call counters: %+v", result)
	}
	if result.RootFunction != "execute(bytes,bytes[])" ||
		len(result.MulticallSelectors) != 1 ||
		result.MulticallSelectors[0] != "0x3593564c" {
		t.Fatalf("unexpected multicall summary: %+v", result)
	}
	if len(result.RouterAddresses) != 2 {
		t.Fatalf("expected root and delegate target routers, got %+v", result.RouterAddresses)
	}
	if !result.Calls[0].IsRouterCall || !result.Calls[0].IsMulticall {
		t.Fatalf("unexpected root classification: %+v", result.Calls[0])
	}
	if !result.Calls[2].IsPoolCall || result.Calls[2].TraceID != candidate.TransactionHash+":0.0" {
		t.Fatalf("unexpected pool call: %+v", result.Calls[2])
	}
}

func testCandidate() Candidate {
	return Candidate{
		ChainID:          8453,
		BlockNumber:      10,
		BlockHash:        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		BlockTime:        time.Now().UTC(),
		TransactionHash:  "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TransactionIndex: 1,
		WalletAddress:    "0x1111111111111111111111111111111111111111",
		TargetAddress:    "0x2222222222222222222222222222222222222222",
		PoolAddresses:    []string{"0x5555555555555555555555555555555555555555"},
	}
}
