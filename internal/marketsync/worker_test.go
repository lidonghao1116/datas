package marketsync

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/basewatch/base-analytics/internal/marketdata"
)

func TestWorkerSyncsAllMarketsAndRotatesRiskBatch(t *testing.T) {
	tokens := []marketdata.Token{
		{ChainID: 8453, Address: "0x01"},
		{ChainID: 8453, Address: "0x02"},
		{ChainID: 8453, Address: "0x03"},
	}
	source := &fakeTokenSource{tokens: tokens}
	provider := &fakeProvider{}
	store := &fakeSnapshotStore{}
	worker, err := New(
		source,
		provider,
		store,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		8453,
		2,
		1,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.markets) != 3 {
		t.Fatalf("expected three market snapshots, got %d", len(store.markets))
	}
	if len(store.risks) != 1 || store.risks[0].Address != "0x01" {
		t.Fatalf("unexpected first risk batch %+v", store.risks)
	}
	if err := worker.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.risks) != 2 || store.risks[1].Address != "0x02" {
		t.Fatalf("risk cursor did not advance: %+v", store.risks)
	}
}

type fakeTokenSource struct {
	tokens []marketdata.Token
}

func (s *fakeTokenSource) ListMarketTokens(
	_ context.Context,
	_ uint64,
	after string,
	limit int,
) ([]marketdata.Token, error) {
	start := 0
	for start < len(s.tokens) && s.tokens[start].Address <= after {
		start++
	}
	end := start + limit
	if end > len(s.tokens) {
		end = len(s.tokens)
	}
	return s.tokens[start:end], nil
}

type fakeProvider struct{}

func (p *fakeProvider) MarketSnapshots(
	_ context.Context,
	tokens []marketdata.Token,
) ([]marketdata.MarketSnapshot, error) {
	result := make([]marketdata.MarketSnapshot, 0, len(tokens))
	for _, token := range tokens {
		result = append(result, marketdata.MarketSnapshot{Token: token, Source: "fake"})
	}
	return result, nil
}

func (p *fakeProvider) RiskSnapshot(
	_ context.Context,
	token marketdata.Token,
) (marketdata.RiskSnapshot, error) {
	return marketdata.RiskSnapshot{Token: token, Source: "fake"}, nil
}

type fakeSnapshotStore struct {
	markets []marketdata.MarketSnapshot
	risks   []marketdata.RiskSnapshot
}

func (s *fakeSnapshotStore) InsertMarketSnapshots(
	_ context.Context,
	snapshots []marketdata.MarketSnapshot,
) error {
	s.markets = append(s.markets, snapshots...)
	return nil
}

func (s *fakeSnapshotStore) InsertRiskSnapshots(
	_ context.Context,
	snapshots []marketdata.RiskSnapshot,
) error {
	s.risks = append(s.risks, snapshots...)
	return nil
}
