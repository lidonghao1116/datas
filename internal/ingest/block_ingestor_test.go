package ingest

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/basewatch/base-analytics/internal/chain/base"
	"github.com/basewatch/base-analytics/internal/domain"
)

type fakeChain struct {
	latest  uint64
	fetched []uint64
}

func (f *fakeChain) LatestBlockNumber(context.Context) (uint64, error) {
	return f.latest, nil
}

func (f *fakeChain) FetchBlock(_ context.Context, number uint64) (domain.RawBlockEnvelope, error) {
	f.fetched = append(f.fetched, number)
	return domain.RawBlockEnvelope{
		SchemaVersion: domain.RawBlockSchemaVersion,
		ChainID:       8453,
		BlockNumber:   number,
		BlockHash:     "0xblock",
		ParentHash:    "0xparent",
		BlockTime:     time.Unix(int64(number), 0),
		ObservedAt:    time.Now(),
		Provider:      "fake",
		Block:         json.RawMessage(`{"transactions":[]}`),
		Receipts:      []json.RawMessage{},
	}, nil
}

func (f *fakeChain) SubscribeHeads(context.Context) (<-chan base.Head, <-chan error, func(), error) {
	return make(chan base.Head), make(chan error), func() {}, nil
}

type fakePublisher struct {
	blocks []uint64
}

func (p *fakePublisher) Publish(_ context.Context, envelope domain.RawBlockEnvelope) error {
	p.blocks = append(p.blocks, envelope.BlockNumber)
	return nil
}

type fakeCheckpoint struct {
	block  uint64
	exists bool
	saved  []uint64
}

func (c *fakeCheckpoint) Load(context.Context, string, uint64) (uint64, bool, error) {
	return c.block, c.exists, nil
}

func (c *fakeCheckpoint) Save(_ context.Context, _ string, _ uint64, block uint64, _ string) error {
	c.saved = append(c.saved, block)
	return nil
}

func TestNextBlockUsesCheckpoint(t *testing.T) {
	chain := &fakeChain{latest: 200}
	checkpoints := &fakeCheckpoint{block: 150, exists: true}
	ingestor := testIngestor(chain, &fakePublisher{}, checkpoints, 100)

	next, err := ingestor.nextBlock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next != 151 {
		t.Fatalf("expected block 151, got %d", next)
	}
}

func TestProcessRangePublishesSequentially(t *testing.T) {
	chain := &fakeChain{}
	publisher := &fakePublisher{}
	checkpoints := &fakeCheckpoint{}
	ingestor := testIngestor(chain, publisher, checkpoints, 100)
	next := uint64(100)

	if err := ingestor.processRange(context.Background(), &next, 102); err != nil {
		t.Fatal(err)
	}
	assertBlocks(t, chain.fetched, []uint64{100, 101, 102})
	assertBlocks(t, publisher.blocks, []uint64{100, 101, 102})
	assertBlocks(t, checkpoints.saved, []uint64{100, 101, 102})
	if next != 103 {
		t.Fatalf("expected next block 103, got %d", next)
	}
}

func testIngestor(
	chain ChainClient,
	publisher *fakePublisher,
	checkpoints *fakeCheckpoint,
	start uint64,
) *BlockIngestor {
	return NewBlockIngestor(
		chain,
		publisher,
		checkpoints,
		8453,
		start,
		time.Millisecond,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

func assertBlocks(t *testing.T, actual, expected []uint64) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	for index := range expected {
		if actual[index] != expected[index] {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	}
}
