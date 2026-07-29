package walletenrichment

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

type fakeStore struct {
	candidates []Candidate
	snapshots  []Snapshot
	states     []SyncState
}

func (s *fakeStore) WalletEnrichmentCandidates(
	context.Context,
	string,
	string,
	time.Duration,
	time.Duration,
	int,
) ([]Candidate, error) {
	return s.candidates, nil
}

func (s *fakeStore) InsertWalletEnrichmentSnapshot(
	_ context.Context,
	snapshot Snapshot,
) error {
	s.snapshots = append(s.snapshots, snapshot)
	return nil
}

func (s *fakeStore) RecordWalletEnrichmentSync(
	_ context.Context,
	state SyncState,
) error {
	s.states = append(s.states, state)
	return nil
}

type fakeClient struct {
	stats Stats
	err   error
}

func (c *fakeClient) WalletStats(
	context.Context,
	string,
	string,
	string,
) (Stats, error) {
	return c.stats, c.err
}

func newTestWorker(t *testing.T, store Store, client Client) *Worker {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	worker, err := NewWorker(
		store,
		client,
		logger,
		[]string{"7d"},
		5,
		time.Hour,
		24*time.Hour,
		time.Second,
		5*time.Minute,
	)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	return worker
}

func TestWorkerStoresSnapshotAndSuccessState(t *testing.T) {
	store := &fakeStore{candidates: []Candidate{{
		ChainID:       8453,
		WalletAddress: "0xabc0000000000000000000000000000000000000",
		Period:        "7d",
	}}}
	worker := newTestWorker(t, store, &fakeClient{stats: Stats{
		RealizedProfitRaw: "10",
	}})

	processed, err := worker.processPeriod(context.Background(), "7d")
	if err != nil {
		t.Fatalf("process period: %v", err)
	}
	if processed != 1 || len(store.snapshots) != 1 || len(store.states) != 1 {
		t.Fatalf(
			"processed=%d snapshots=%d states=%d",
			processed,
			len(store.snapshots),
			len(store.states),
		)
	}
	if store.states[0].Status != "success" ||
		store.snapshots[0].Stats.WalletAddress != store.candidates[0].WalletAddress {
		t.Fatalf("unexpected enrichment result: %+v %+v", store.snapshots[0], store.states[0])
	}
}

func TestWorkerRecordsFailureWithoutSnapshot(t *testing.T) {
	store := &fakeStore{candidates: []Candidate{{
		ChainID:       8453,
		WalletAddress: "0xabc0000000000000000000000000000000000000",
		Period:        "7d",
	}}}
	worker := newTestWorker(t, store, &fakeClient{err: errors.New("upstream unavailable")})

	processed, err := worker.processPeriod(context.Background(), "7d")
	if err != nil {
		t.Fatalf("process period: %v", err)
	}
	if processed != 1 || len(store.snapshots) != 0 || store.states[0].Status != "failed" {
		t.Fatalf("unexpected failure handling: %+v", store.states)
	}
	if !store.states[0].NextRetryAt.After(store.states[0].AttemptedAt) {
		t.Fatal("failure did not schedule a retry")
	}
}
