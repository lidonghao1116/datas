package walletprofile

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

type fakeStore struct {
	candidates []Candidate
	inserted   []Activity
}

func (s *fakeStore) WalletActivityCandidates(context.Context, int) ([]Candidate, error) {
	return s.candidates, nil
}

func (s *fakeStore) InsertWalletActivities(_ context.Context, activities []Activity) error {
	s.inserted = append(s.inserted, activities...)
	return nil
}

func TestWorkerBuildsNormalizedWalletActivity(t *testing.T) {
	store := &fakeStore{candidates: []Candidate{
		{
			EventID:            "event-1",
			WalletAddress:      "0xAbC0000000000000000000000000000000000000",
			RouterAddress:      "0xDEF0000000000000000000000000000000000000",
			BoughtTokenAddress: "0x4200000000000000000000000000000000000006",
			SoldTokenAddress:   "0x0000000000000000000000000000000000000001",
			SourceValuedAt:     time.Now().UTC(),
		},
	}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	worker, err := NewWorker(store, logger, 10, time.Second)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}

	scanned, inserted, skipped, err := worker.processBatch(context.Background())
	if err != nil {
		t.Fatalf("process batch: %v", err)
	}
	if scanned != 1 || inserted != 1 || skipped != 0 {
		t.Fatalf("unexpected counts: %d %d %d", scanned, inserted, skipped)
	}
	if got := store.inserted[0].WalletAddress; got != "0xabc0000000000000000000000000000000000000" {
		t.Fatalf("wallet address = %q", got)
	}
	if store.inserted[0].AttributionMethod != AttributionTransactionFrom {
		t.Fatalf("attribution method = %q", store.inserted[0].AttributionMethod)
	}
}

func TestWorkerSkipsInvalidCandidate(t *testing.T) {
	store := &fakeStore{candidates: []Candidate{{EventID: "invalid"}}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	worker, err := NewWorker(store, logger, 10, time.Second)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}

	scanned, inserted, skipped, err := worker.processBatch(context.Background())
	if err != nil {
		t.Fatalf("process batch: %v", err)
	}
	if scanned != 1 || inserted != 0 || skipped != 1 {
		t.Fatalf("unexpected counts: %d %d %d", scanned, inserted, skipped)
	}
}
