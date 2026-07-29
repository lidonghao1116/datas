package domain

import "testing"

func TestEventMetaEventID(t *testing.T) {
	meta := EventMeta{
		ChainID:         8453,
		BlockHash:       "0xblock",
		TransactionHash: "0xtx",
		LogIndex:        7,
	}
	if got, want := meta.EventID(), "8453:0xblock:0xtx:7"; got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
