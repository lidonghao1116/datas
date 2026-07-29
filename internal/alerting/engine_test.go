package alerting

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestEngineClassifiesLargeBuyAndRiskSeverity(t *testing.T) {
	engine := testEngine(t)
	alert, err := engine.buildAlert(Candidate{
		EventID:            "event-1",
		ValuationVersion:   "usd-v2",
		ChainID:            8453,
		TradeValueUSDRaw:   "12000",
		BoughtTokenAddress: "0xtoken",
		BoughtTokenSymbol:  "MEME",
		SoldTokenAddress:   "0xusdc",
		SoldTokenSymbol:    "USDC",
		BoughtRisk:         RiskFlags{HasMintMethod: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if alert.Type != "large_buy" || alert.TokenSymbol != "MEME" {
		t.Fatalf("unexpected alert classification %+v", alert)
	}
	if alert.Severity != "high" {
		t.Fatalf("expected high severity, got %s", alert.Severity)
	}
}

func TestEnginePromotesHoneypotToCritical(t *testing.T) {
	engine := testEngine(t)
	alert, err := engine.buildAlert(Candidate{
		EventID:            "event-2",
		ValuationVersion:   "usd-v2",
		ChainID:            8453,
		TradeValueUSDRaw:   "10001",
		BoughtTokenAddress: "0xusdc",
		BoughtTokenSymbol:  "USDC",
		SoldTokenAddress:   "0xtoken",
		SoldTokenSymbol:    "SCAM",
		SoldRisk:           RiskFlags{IsHoneypot: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if alert.Type != "large_sell" || alert.Severity != "critical" {
		t.Fatalf("unexpected risky sell alert %+v", alert)
	}
}

func TestSortedQuoteSymbolsNormalizesAndDeduplicates(t *testing.T) {
	values := SortedQuoteSymbols(" weth,USDC,weth, cbBTC ")
	expected := []string{"CBBTC", "USDC", "WETH"}
	if len(values) != len(expected) {
		t.Fatalf("unexpected values %v", values)
	}
	for index := range expected {
		if values[index] != expected[index] {
			t.Fatalf("unexpected values %v", values)
		}
	}
}

func testEngine(t *testing.T) *Engine {
	t.Helper()
	engine, err := NewEngine(
		fakeCandidateStore{},
		fakeOutbox{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		[]string{"WETH", "USDC", "cbBTC"},
		"50000",
		time.Hour,
		100,
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

type fakeCandidateStore struct{}

func (fakeCandidateStore) LargeTradeCandidates(
	context.Context,
	time.Duration,
	Cursor,
	int,
) ([]Candidate, error) {
	return nil, nil
}

type fakeOutbox struct{}

func (fakeOutbox) InsertAlerts(context.Context, []Alert) (int, error) {
	return 0, nil
}
