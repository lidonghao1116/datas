package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/basewatch/base-analytics/internal/chain/base"
	"github.com/basewatch/base-analytics/internal/checkpoint"
	"github.com/basewatch/base-analytics/internal/domain"
)

type fakeChain struct {
	latest  uint64
	fetched []uint64
	blocks  map[uint64]domain.RawBlockEnvelope
}

func (f *fakeChain) LatestBlockNumber(context.Context) (uint64, error) {
	return f.latest, nil
}

func (f *fakeChain) FetchBlock(_ context.Context, number uint64) (domain.RawBlockEnvelope, error) {
	f.fetched = append(f.fetched, number)
	if block, exists := f.blocks[number]; exists {
		return block, nil
	}
	return domain.RawBlockEnvelope{
		SchemaVersion: domain.RawBlockSchemaVersion,
		ChainID:       8453,
		BlockNumber:   number,
		BlockHash:     fmt.Sprintf("0xblock%d", number),
		ParentHash:    fmt.Sprintf("0xblock%d", number-1),
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
	blocks    []uint64
	envelopes []domain.RawBlockEnvelope
	err       error
}

func (p *fakePublisher) Publish(_ context.Context, envelope domain.RawBlockEnvelope) error {
	if p.err != nil {
		return p.err
	}
	p.blocks = append(p.blocks, envelope.BlockNumber)
	p.envelopes = append(p.envelopes, envelope)
	return nil
}

type fakeCheckpoint struct {
	point   checkpoint.Point
	exists  bool
	saved   []uint64
	headers map[uint64]string
	rewound []uint64
}

func (c *fakeCheckpoint) Load(context.Context, string, uint64) (checkpoint.Point, bool, error) {
	return c.point, c.exists, nil
}

func (c *fakeCheckpoint) Header(
	_ context.Context,
	_ string,
	_, blockNumber uint64,
) (checkpoint.Point, bool, error) {
	hash, exists := c.headers[blockNumber]
	return checkpoint.Point{BlockNumber: blockNumber, BlockHash: hash}, exists, nil
}

func (c *fakeCheckpoint) Range(
	_ context.Context,
	_ string,
	_, fromBlock, toBlock uint64,
) ([]checkpoint.Point, error) {
	points := make([]checkpoint.Point, 0, toBlock-fromBlock+1)
	for number := fromBlock; number <= toBlock; number++ {
		if hash, exists := c.headers[number]; exists {
			points = append(points, checkpoint.Point{BlockNumber: number, BlockHash: hash})
		}
	}
	return points, nil
}

func (c *fakeCheckpoint) Save(
	_ context.Context,
	_ string,
	_ uint64,
	block uint64,
	hash string,
) error {
	c.saved = append(c.saved, block)
	if c.headers == nil {
		c.headers = make(map[uint64]string)
	}
	c.headers[block] = hash
	c.point = checkpoint.Point{BlockNumber: block, BlockHash: hash}
	c.exists = true
	return nil
}

func (c *fakeCheckpoint) Rewind(
	_ context.Context,
	_ string,
	_ uint64,
	ancestor checkpoint.Point,
) error {
	c.rewound = append(c.rewound, ancestor.BlockNumber)
	for number := range c.headers {
		if number > ancestor.BlockNumber {
			delete(c.headers, number)
		}
	}
	c.point = ancestor
	return nil
}

func TestNextBlockUsesCheckpoint(t *testing.T) {
	chain := &fakeChain{latest: 200}
	checkpoints := &fakeCheckpoint{
		point:  checkpoint.Point{BlockNumber: 150, BlockHash: "0x150"},
		exists: true,
	}
	ingestor := testIngestor(chain, &fakePublisher{}, checkpoints, 100)

	next, err := ingestor.nextBlock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next != 151 {
		t.Fatalf("expected block 151, got %d", next)
	}
}

func TestProcessRangeRewindsAndPublishesReplacementBranch(t *testing.T) {
	chain := &fakeChain{
		blocks: map[uint64]domain.RawBlockEnvelope{
			101: blockEnvelope(101, "0xnew101", "0x100"),
			102: blockEnvelope(102, "0xnew102", "0xnew101"),
			103: blockEnvelope(103, "0xnew103", "0xnew102"),
		},
	}
	publisher := &fakePublisher{}
	checkpoints := &fakeCheckpoint{
		point:  checkpoint.Point{BlockNumber: 102, BlockHash: "0xold102"},
		exists: true,
		headers: map[uint64]string{
			100: "0x100",
			101: "0xold101",
			102: "0xold102",
		},
	}
	ingestor := testIngestor(chain, publisher, checkpoints, 100)
	next := uint64(103)

	if err := ingestor.processRange(context.Background(), &next, 103); err != nil {
		t.Fatal(err)
	}

	assertBlocks(t, checkpoints.rewound, []uint64{100})
	assertBlocks(t, publisher.blocks, []uint64{101, 102, 103})
	if next != 104 {
		t.Fatalf("expected next block 104, got %d", next)
	}
	reorganization := publisher.envelopes[0].Reorganization
	if reorganization == nil {
		t.Fatal("expected replacement branch first block to carry reorganization")
	}
	if reorganization.CommonAncestor.BlockNumber != 100 ||
		reorganization.OldHead.BlockNumber != 102 ||
		len(reorganization.OrphanedBlocks) != 2 {
		t.Fatalf("unexpected reorganization: %+v", reorganization)
	}
	if publisher.envelopes[1].Reorganization != nil ||
		publisher.envelopes[2].Reorganization != nil {
		t.Fatal("expected only first replacement block to carry reorganization")
	}
}

func TestDetectReorganizationRejectsDepthBeyondLimit(t *testing.T) {
	chain := &fakeChain{
		blocks: map[uint64]domain.RawBlockEnvelope{
			102: blockEnvelope(102, "0xnew102", "0xnew101"),
		},
	}
	checkpoints := &fakeCheckpoint{
		point:  checkpoint.Point{BlockNumber: 103, BlockHash: "0xold103"},
		exists: true,
		headers: map[uint64]string{
			101: "0xold101",
			102: "0xold102",
			103: "0xold103",
		},
	}
	ingestor := NewBlockIngestor(
		chain,
		&fakePublisher{},
		checkpoints,
		8453,
		100,
		1,
		time.Millisecond,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	_, err := ingestor.detectReorganization(
		context.Background(),
		blockEnvelope(104, "0xnew104", "0xnew103"),
	)
	if err == nil {
		t.Fatal("expected reorganization depth error")
	}
}

func TestReorganizationDoesNotRewindBeforeReplacementIsPublished(t *testing.T) {
	chain := &fakeChain{
		blocks: map[uint64]domain.RawBlockEnvelope{
			101: blockEnvelope(101, "0xnew101", "0x100"),
			102: blockEnvelope(102, "0xnew102", "0xnew101"),
			103: blockEnvelope(103, "0xnew103", "0xnew102"),
		},
	}
	publisher := &fakePublisher{err: errors.New("publish failed")}
	checkpoints := &fakeCheckpoint{
		point: checkpoint.Point{BlockNumber: 102, BlockHash: "0xold102"},
		exists: true,
		headers: map[uint64]string{
			100: "0x100",
			101: "0xold101",
			102: "0xold102",
		},
	}
	ingestor := testIngestor(chain, publisher, checkpoints, 100)
	next := uint64(103)

	if err := ingestor.processRange(context.Background(), &next, 103); err == nil {
		t.Fatal("expected publish error")
	}
	if len(checkpoints.rewound) != 0 {
		t.Fatalf("checkpoint rewound before publish: %v", checkpoints.rewound)
	}
	if checkpoints.point.BlockNumber != 102 {
		t.Fatalf("expected checkpoint to remain at 102, got %d", checkpoints.point.BlockNumber)
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
		128,
		time.Millisecond,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

func blockEnvelope(number uint64, hash, parentHash string) domain.RawBlockEnvelope {
	return domain.RawBlockEnvelope{
		SchemaVersion: domain.RawBlockSchemaVersion,
		ChainID:       8453,
		BlockNumber:   number,
		BlockHash:     hash,
		ParentHash:    parentHash,
		BlockTime:     time.Unix(int64(number), 0),
		ObservedAt:    time.Now(),
		Provider:      "fake",
		Block:         json.RawMessage(`{"transactions":[]}`),
		Receipts:      []json.RawMessage{},
	}
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
