package walletanalytics

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

type fakeStore struct {
	candidates []Candidate
	input      Input
	results    []Result
}

func (s *fakeStore) WalletAnalysisCandidates(
	context.Context,
	string,
	int,
) ([]Candidate, error) {
	return s.candidates, nil
}

func (s *fakeStore) LoadWalletAnalysis(context.Context, Candidate) (Input, error) {
	return s.input, nil
}

func (s *fakeStore) InsertWalletAnalysis(_ context.Context, result Result) error {
	s.results = append(s.results, result)
	return nil
}

func TestWorkerProcessesCandidate(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeStore{
		candidates: []Candidate{{
			ChainID:       8453,
			WalletAddress: "0xabc0000000000000000000000000000000000000",
		}},
		input: Input{
			ChainID:       8453,
			WalletAddress: "0xabc0000000000000000000000000000000000000",
			Trades: []Trade{{
				EventID:            "event",
				BlockTime:          now,
				BoughtTokenAddress: "0xtoken",
				BoughtTokenSymbol:  "TKN",
				BoughtAmountRaw:    "1",
				SoldTokenAddress:   "0xusdc",
				SoldTokenSymbol:    "USDC",
				SoldAmountRaw:      "10",
				TradeValueUSDRaw:   "10",
				ValuationStatus:    "valued",
				GeneratedAt:        now,
			}},
			Prices: map[string]Price{},
			Risks:  map[string]Risk{},
		},
	}
	calculator, err := NewCalculator([]string{"USDC"}, time.Hour)
	if err != nil {
		t.Fatalf("new calculator: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	worker, err := NewWorker(store, calculator, logger, 10, time.Second)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	processed, err := worker.processBatch(context.Background())
	if err != nil {
		t.Fatalf("process batch: %v", err)
	}
	if processed != 1 || len(store.results) != 1 {
		t.Fatalf("processed=%d results=%d", processed, len(store.results))
	}
	if store.results[0].Score.AnalyticsVersion != Version {
		t.Fatalf("analytics version = %q", store.results[0].Score.AnalyticsVersion)
	}
}
