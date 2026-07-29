package backfill

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/basewatch/base-analytics/internal/domain"
)

func TestRegistryWorkerUpsertsOnlyEnrichedSwaps(t *testing.T) {
	store := &fakeStore{
		pending: []domain.PoolSwap{
			newSwap(100, 1),
			newSwap(101, 2),
		},
	}
	enricher := fakeEnricher{}
	worker, err := NewRegistryWorker(
		store,
		enricher,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		10,
		time.Second,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}

	processed, cursor, err := worker.processBatch(context.Background(), Cursor{})
	if err != nil {
		t.Fatal(err)
	}
	if processed != 2 || cursor.BlockNumber != 101 {
		t.Fatalf("unexpected progress processed=%d cursor=%+v", processed, cursor)
	}
	if len(store.upserted) != 1 {
		t.Fatalf("expected one enriched swap, got %d", len(store.upserted))
	}
	if store.upserted[0].MetadataStatus != "resolved" {
		t.Fatalf("unexpected status %s", store.upserted[0].MetadataStatus)
	}
	if !store.upserted[0].ObservedAt.After(store.pending[0].ObservedAt) {
		t.Fatal("backfill must advance observed_at for ReplacingMergeTree")
	}
}

func TestRegistryWorkerValidatesConfiguration(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := NewRegistryWorker(nil, fakeEnricher{}, logger, 1, time.Second, time.Second); err == nil {
		t.Fatal("expected missing store error")
	}
	if _, err := NewRegistryWorker(&fakeStore{}, nil, logger, 1, time.Second, time.Second); err == nil {
		t.Fatal("expected missing enricher error")
	}
	if _, err := NewRegistryWorker(&fakeStore{}, fakeEnricher{}, logger, 0, time.Second, time.Second); err == nil {
		t.Fatal("expected invalid batch size error")
	}
}

func newSwap(blockNumber uint64, logIndex uint32) domain.PoolSwap {
	return domain.PoolSwap{
		EventMeta: domain.EventMeta{
			ChainID:         8453,
			BlockNumber:     blockNumber,
			BlockHash:       "0xblock",
			TransactionHash: "0xtx",
			LogIndex:        logIndex,
			ObservedAt:      time.Unix(1, 0).UTC(),
		},
		PoolAddress:    "0x1111111111111111111111111111111111111111",
		MetadataStatus: "unresolved",
	}
}

type fakeStore struct {
	pending  []domain.PoolSwap
	upserted []domain.PoolSwap
}

func (s *fakeStore) PendingSwaps(
	_ context.Context,
	_ Cursor,
	limit int,
) ([]domain.PoolSwap, error) {
	if limit < len(s.pending) {
		return s.pending[:limit], nil
	}
	return s.pending, nil
}

func (s *fakeStore) UpsertSwaps(_ context.Context, swaps []domain.PoolSwap) error {
	s.upserted = append(s.upserted, swaps...)
	return nil
}

type fakeEnricher struct{}

func (fakeEnricher) EnrichSwaps(_ context.Context, swaps []domain.PoolSwap) []error {
	swaps[0].MetadataStatus = "resolved"
	swaps[0].Token0Symbol = "USDC"
	return nil
}
