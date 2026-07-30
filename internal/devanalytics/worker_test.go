package devanalytics

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestWorkerProcessesDevCandidate(t *testing.T) {
	now := time.Now().UTC()
	candidate := Candidate{
		ChainID:               8453,
		TokenAddress:          "0x1111111111111111111111111111111111111111",
		PrimaryDeployer:       "0x2222222222222222222222222222222222222222",
		DeploymentTransaction: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		DeploymentBlock:       10,
		DeployedAt:            now,
	}
	store := &fakeDevStore{
		candidates: []Candidate{candidate},
		input:      Input{Candidate: candidate},
	}
	worker, err := NewWorker(
		store,
		NewCalculator(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		WorkerConfig{
			ChainID:         8453,
			BatchSize:       10,
			EvidenceLimit:   20,
			PollInterval:    time.Second,
			RefreshInterval: time.Hour,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.processBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 || len(store.results) != 1 {
		t.Fatalf("unexpected worker result: processed=%d results=%d", processed, len(store.results))
	}
}

type fakeDevStore struct {
	candidates []Candidate
	input      Input
	results    []Result
}

func (s *fakeDevStore) DevAnalysisCandidates(
	context.Context,
	string,
	uint64,
	time.Time,
	int,
) ([]Candidate, error) {
	return s.candidates, nil
}

func (s *fakeDevStore) LoadDevAnalysis(
	context.Context,
	Candidate,
	int,
) (Input, error) {
	return s.input, nil
}

func (s *fakeDevStore) InsertDevAnalysis(_ context.Context, result Result) error {
	s.results = append(s.results, result)
	return nil
}
