package walletanalytics

import (
	"testing"
	"time"
)

func TestCalculatorWeightedCostRealizedAndUnrealizedPnL(t *testing.T) {
	now := time.Now().UTC()
	calculator, err := NewCalculator([]string{"USDC", "WETH"}, time.Hour)
	if err != nil {
		t.Fatalf("new calculator: %v", err)
	}
	result, err := calculator.Calculate(Input{
		ChainID:       8453,
		WalletAddress: "0xabc0000000000000000000000000000000000000",
		Trades: []Trade{
			{
				EventID:            "buy",
				BlockTime:          now.Add(-2 * time.Minute),
				BoughtTokenAddress: "0xtoken",
				BoughtTokenSymbol:  "TKN",
				BoughtAmountRaw:    "10",
				SoldTokenAddress:   "0xusdc",
				SoldTokenSymbol:    "USDC",
				SoldAmountRaw:      "100",
				TradeValueUSDRaw:   "100",
				ValuationStatus:    "valued",
				GeneratedAt:        now.Add(-2 * time.Minute),
			},
			{
				EventID:            "sell",
				BlockTime:          now.Add(-time.Minute),
				BoughtTokenAddress: "0xusdc",
				BoughtTokenSymbol:  "USDC",
				BoughtAmountRaw:    "60",
				SoldTokenAddress:   "0xtoken",
				SoldTokenSymbol:    "TKN",
				SoldAmountRaw:      "4",
				TradeValueUSDRaw:   "60",
				ValuationStatus:    "valued",
				GeneratedAt:        now.Add(-time.Minute),
			},
		},
		Prices: map[string]Price{
			"0xtoken": {Raw: "12", UpdatedAt: now},
		},
		Risks: map[string]Risk{},
	}, now)
	if err != nil {
		t.Fatalf("calculate: %v", err)
	}
	token := findToken(t, result.Tokens, "0xtoken")
	if token.RemainingAmountRaw != "6" ||
		token.RemainingCostUSDRaw != "60" ||
		token.RealizedProfitUSDRaw != "20" ||
		token.UnrealizedProfitUSDRaw != "12" ||
		token.TotalProfitUSDRaw != "32" {
		t.Fatalf("unexpected token PnL: %+v", token)
	}
	if result.Score.TotalProfitUSDRaw != "32" ||
		result.Score.ROIRaw != "0.32" ||
		result.Score.WinRateRaw != "1" ||
		result.Score.ClosedSellCount != 1 {
		t.Fatalf("unexpected wallet score: %+v", result.Score)
	}
}

func TestCalculatorMarksUnmatchedSellAndMissingPrice(t *testing.T) {
	now := time.Now().UTC()
	calculator, err := NewCalculator([]string{"USDC"}, time.Hour)
	if err != nil {
		t.Fatalf("new calculator: %v", err)
	}
	result, err := calculator.Calculate(Input{
		ChainID:       8453,
		WalletAddress: "0xabc0000000000000000000000000000000000000",
		Trades: []Trade{
			{
				EventID:            "sell-before-history",
				BlockTime:          now.Add(-time.Minute),
				BoughtTokenAddress: "0xusdc",
				BoughtTokenSymbol:  "USDC",
				BoughtAmountRaw:    "50",
				SoldTokenAddress:   "0xtoken",
				SoldTokenSymbol:    "TKN",
				SoldAmountRaw:      "5",
				TradeValueUSDRaw:   "50",
				ValuationStatus:    "single_sided",
				GeneratedAt:        now.Add(-time.Minute),
			},
		},
		Prices: map[string]Price{},
		Risks: map[string]Risk{
			"0xtoken": {IsHoneypot: true},
		},
		TransferInCount: 1,
	}, now)
	if err != nil {
		t.Fatalf("calculate: %v", err)
	}
	token := findToken(t, result.Tokens, "0xtoken")
	if token.UnmatchedSellAmountRaw != "5" ||
		token.UnmatchedSellUSDRaw != "50" ||
		token.DataQuality != "incomplete_history" {
		t.Fatalf("unexpected unmatched sell: %+v", token)
	}
	if !result.Score.HistoryIncomplete ||
		result.Score.UnmatchedSellCount != 1 ||
		result.Score.PartialValuationCount != 1 ||
		result.Score.RiskyTokenCount != 1 {
		t.Fatalf("unexpected quality score: %+v", result.Score)
	}
}

func findToken(t *testing.T, tokens []TokenPnL, address string) TokenPnL {
	t.Helper()
	for _, token := range tokens {
		if token.TokenAddress == address {
			return token
		}
	}
	t.Fatalf("token %s not found", address)
	return TokenPnL{}
}
