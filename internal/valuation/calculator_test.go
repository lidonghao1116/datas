package valuation

import (
	"testing"
	"time"

	"github.com/basewatch/base-analytics/internal/domain"
)

func TestCalculatorValuesSwapAndAssignsDirection(t *testing.T) {
	now := time.Now().UTC()
	calculator, err := NewCalculator(10*time.Minute, "500")
	if err != nil {
		t.Fatal(err)
	}
	candidate := Candidate{
		Swap: domain.PoolSwap{
			EventMeta: domain.EventMeta{
				ChainID:         8453,
				BlockHash:       "0xblock",
				TransactionHash: "0xtx",
				LogIndex:        1,
				ObservedAt:      now,
			},
			Token0Address:   "0xusdc",
			Token1Address:   "0xweth",
			Token0Symbol:    "USDC",
			Token1Symbol:    "WETH",
			Token0Decimals:  6,
			Token1Decimals:  18,
			Amount0DeltaRaw: "1000000000",
			Amount1DeltaRaw: "-500000000000000000",
		},
		Price0: Price{
			Raw:             "1",
			Source:          "ave",
			SourceUpdatedAt: now,
			FetchedAt:       now,
		},
		Price1: Price{
			Raw:             "2000",
			Source:          "ave",
			SourceUpdatedAt: now,
			FetchedAt:       now,
		},
	}

	result, valued, err := calculator.Value(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !valued {
		t.Fatal("expected swap to be valued")
	}
	if result.Token0AmountRaw != "1000" || result.Token1AmountRaw != "0.5" {
		t.Fatalf("unexpected normalized amounts %s/%s", result.Token0AmountRaw, result.Token1AmountRaw)
	}
	if result.TradeValueUSDRaw != "1000" {
		t.Fatalf("unexpected trade value %s", result.TradeValueUSDRaw)
	}
	if result.BoughtTokenSymbol != "WETH" || result.SoldTokenSymbol != "USDC" {
		t.Fatalf("unexpected direction %s/%s", result.BoughtTokenSymbol, result.SoldTokenSymbol)
	}
	if result.IsLargeTrade != 1 || result.Status != "valued" {
		t.Fatalf("unexpected classification %+v", result)
	}
}

func TestCalculatorUsesFreshSingleSidedPrice(t *testing.T) {
	now := time.Now().UTC()
	calculator, err := NewCalculator(time.Minute, "10000")
	if err != nil {
		t.Fatal(err)
	}
	candidate := Candidate{
		Swap: domain.PoolSwap{
			EventMeta:       domain.EventMeta{ObservedAt: now},
			Token0Decimals:  6,
			Token1Decimals:  18,
			Amount0DeltaRaw: "1000000",
			Amount1DeltaRaw: "-1000000000000000000",
		},
		Price0: Price{Raw: "1", Source: "ave", FetchedAt: now},
		Price1: Price{Raw: "2000", Source: "ave", FetchedAt: now.Add(-2 * time.Minute)},
	}

	result, valued, err := calculator.Value(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !valued || result.Status != "single_sided" || result.TradeValueUSDRaw != "1" {
		t.Fatalf("unexpected single-sided result %+v valued=%v", result, valued)
	}
}

func TestCalculatorRejectsInvalidRawAmount(t *testing.T) {
	calculator, err := NewCalculator(time.Minute, "100")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = calculator.Value(Candidate{
		Swap: domain.PoolSwap{
			EventMeta:       domain.EventMeta{ObservedAt: time.Now()},
			Amount0DeltaRaw: "not-an-integer",
			Amount1DeltaRaw: "1",
		},
	})
	if err == nil {
		t.Fatal("expected invalid amount error")
	}
}

func TestCalculatorSuppressesLargeAlertForPriceMismatch(t *testing.T) {
	now := time.Now().UTC()
	calculator, err := NewCalculator(time.Minute, "100")
	if err != nil {
		t.Fatal(err)
	}
	result, valued, err := calculator.Value(Candidate{
		Swap: domain.PoolSwap{
			EventMeta:       domain.EventMeta{ObservedAt: now},
			Token0Decimals:  0,
			Token1Decimals:  0,
			Amount0DeltaRaw: "10",
			Amount1DeltaRaw: "-1000000",
		},
		Price0: Price{Raw: "1", Source: "ave", FetchedAt: now},
		Price1: Price{Raw: "1", Source: "ave", FetchedAt: now},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !valued || result.Status != "price_mismatch" {
		t.Fatalf("expected price mismatch, got %+v", result)
	}
	if result.TradeValueUSDRaw != "10" || result.IsLargeTrade != 0 {
		t.Fatalf("expected conservative non-alerting value, got %+v", result)
	}
}
