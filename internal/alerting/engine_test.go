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

func TestEngineEmitsIndependentLargeAndSmartMoneyAlerts(t *testing.T) {
	engine := testEngine(t)
	alerts, err := engine.buildAlerts(Candidate{
		EventID:            "event-3",
		ValuationVersion:   "usd-v2",
		ChainID:            8453,
		WalletAddress:      "0xsmart",
		TradeValueUSDRaw:   "12000",
		BoughtTokenAddress: "0xtoken",
		BoughtTokenSymbol:  "MEME",
		SoldTokenAddress:   "0xusdc",
		SoldTokenSymbol:    "USDC",
		IsLargeTrade:       true,
		SmartScoreVersion:  "smart-v1",
		SmartScoreRaw:      "72.5",
		SmartScoreGrade:    "B",
		SmartConfidenceRaw: "0.8",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 2 {
		t.Fatalf("expected large and smart-money alerts, got %+v", alerts)
	}
	if alerts[0].Type != "large_buy" || alerts[1].Type != "smart_money_buy" {
		t.Fatalf("unexpected alert types %+v", alerts)
	}
	if alerts[1].Severity != "high" {
		t.Fatalf("expected smart-money alert to be high severity, got %s", alerts[1].Severity)
	}
	if alerts[0].Key == alerts[1].Key {
		t.Fatal("large and smart-money alerts must have distinct idempotency keys")
	}
}

func TestEngineRequiresSmartMoneyConfidenceAndTradeMinimum(t *testing.T) {
	engine := testEngine(t)
	alerts, err := engine.buildAlerts(Candidate{
		EventID:            "event-4",
		ValuationVersion:   "usd-v2",
		ChainID:            8453,
		TradeValueUSDRaw:   "999",
		BoughtTokenAddress: "0xtoken",
		BoughtTokenSymbol:  "MEME",
		SoldTokenAddress:   "0xusdc",
		SoldTokenSymbol:    "USDC",
		SmartScoreVersion:  "smart-v1",
		SmartScoreRaw:      "90",
		SmartConfidenceRaw: "0.59",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 0 {
		t.Fatalf("expected no alert below smart-money gates, got %+v", alerts)
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
		EngineConfig{
			QuoteSymbols:       []string{"WETH", "USDC", "cbBTC"},
			CriticalUSD:        "50000",
			SmartScoreVersion:  "smart-v1",
			SmartScoreMin:      "65",
			SmartConfidenceMin: "0.6",
			SmartTradeMinUSD:   "1000",
			Lookback:           time.Hour,
			BatchSize:          100,
			PollInterval:       time.Second,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

type fakeCandidateStore struct{}

func (fakeCandidateStore) RealtimeAlertCandidates(
	context.Context,
	time.Duration,
	Cursor,
	string,
	int,
) ([]Candidate, error) {
	return nil, nil
}

type fakeOutbox struct{}

func (fakeOutbox) InsertAlerts(context.Context, []Alert) (int, error) {
	return 0, nil
}
