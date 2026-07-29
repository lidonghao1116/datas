package valuation

import (
	"fmt"
	"math/big"
	"strings"
	"time"
)

type Calculator struct {
	maxPriceAge   time.Duration
	largeTradeUSD *big.Rat
}

func NewCalculator(maxPriceAge time.Duration, largeTradeUSD string) (*Calculator, error) {
	if maxPriceAge <= 0 {
		return nil, fmt.Errorf("maximum price age must be positive")
	}
	threshold, ok := new(big.Rat).SetString(largeTradeUSD)
	if !ok || threshold.Sign() < 0 {
		return nil, fmt.Errorf("large trade threshold must be a non-negative decimal")
	}
	return &Calculator{maxPriceAge: maxPriceAge, largeTradeUSD: threshold}, nil
}

func (c *Calculator) Value(candidate Candidate) (Result, bool, error) {
	swap := candidate.Swap
	amount0, delta0, err := normalizedAmount(swap.Amount0DeltaRaw, swap.Token0Decimals)
	if err != nil {
		return Result{}, false, fmt.Errorf("normalize token0 amount: %w", err)
	}
	amount1, delta1, err := normalizedAmount(swap.Amount1DeltaRaw, swap.Token1Decimals)
	if err != nil {
		return Result{}, false, fmt.Errorf("normalize token1 amount: %w", err)
	}

	price0, fresh0 := c.freshPrice(candidate.Price0, swap.ObservedAt)
	price1, fresh1 := c.freshPrice(candidate.Price1, swap.ObservedAt)
	if !fresh0 && !fresh1 {
		return Result{}, false, nil
	}
	var value0, value1 *big.Rat
	if fresh0 {
		value0 = new(big.Rat).Mul(amount0, price0)
	}
	if fresh1 {
		value1 = new(big.Rat).Mul(amount1, price1)
	}
	tradeValue, status := combinedValue(value0, value1)
	if tradeValue == nil {
		return Result{}, false, nil
	}

	result := Result{
		EventID:              swap.EventID(),
		Swap:                 swap,
		Token0AmountRaw:      formatDecimal(amount0),
		Token1AmountRaw:      formatDecimal(amount1),
		Token0ValueUSDRaw:    formatDecimal(value0),
		Token1ValueUSDRaw:    formatDecimal(value1),
		TradeValueUSDRaw:     formatDecimal(tradeValue),
		Status:               status,
		Version:              Version,
		PriceSource:          priceSource(candidate.Price0, candidate.Price1, fresh0, fresh1),
		Token0PriceUpdatedAt: candidate.Price0.SourceUpdatedAt,
		Token1PriceUpdatedAt: candidate.Price1.SourceUpdatedAt,
		ValuedAt:             time.Now().UTC(),
	}
	if fresh0 {
		result.Token0PriceUSDRaw = candidate.Price0.Raw
	}
	if fresh1 {
		result.Token1PriceUSDRaw = candidate.Price1.Raw
	}
	if status == "valued" && tradeValue.Cmp(c.largeTradeUSD) >= 0 {
		result.IsLargeTrade = 1
	}
	assignDirection(&result, delta0.Sign(), delta1.Sign())
	return result, true, nil
}

func (c *Calculator) freshPrice(price Price, reference time.Time) (*big.Rat, bool) {
	if strings.TrimSpace(price.Raw) == "" || price.FetchedAt.IsZero() {
		return nil, false
	}
	age := reference.Sub(price.FetchedAt)
	if age < 0 {
		age = -age
	}
	if age > c.maxPriceAge {
		return nil, false
	}
	value, ok := new(big.Rat).SetString(price.Raw)
	return value, ok && value.Sign() >= 0
}

func normalizedAmount(raw string, decimals uint8) (*big.Rat, *big.Int, error) {
	delta, ok := new(big.Int).SetString(strings.TrimSpace(raw), 10)
	if !ok {
		return nil, nil, fmt.Errorf("invalid integer %q", raw)
	}
	absolute := new(big.Int).Abs(new(big.Int).Set(delta))
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	return new(big.Rat).SetFrac(absolute, scale), delta, nil
}

func combinedValue(value0, value1 *big.Rat) (*big.Rat, string) {
	switch {
	case value0 != nil && value1 != nil:
		minimum := value0
		maximum := value1
		if value0.Cmp(value1) > 0 {
			minimum = value1
			maximum = value0
		}
		if minimum.Sign() == 0 {
			return new(big.Rat).Set(minimum), "price_mismatch"
		}
		ratio := new(big.Rat).Quo(new(big.Rat).Set(maximum), minimum)
		if ratio.Cmp(big.NewRat(3, 1)) > 0 {
			return new(big.Rat).Set(minimum), "price_mismatch"
		}
		total := new(big.Rat).Add(value0, value1)
		return total.Quo(total, big.NewRat(2, 1)), "valued"
	case value0 != nil:
		return new(big.Rat).Set(value0), "single_sided"
	case value1 != nil:
		return new(big.Rat).Set(value1), "single_sided"
	default:
		return nil, ""
	}
}

func assignDirection(result *Result, sign0, sign1 int) {
	switch {
	case sign0 < 0 && sign1 > 0:
		result.BoughtTokenAddress = result.Swap.Token0Address
		result.BoughtTokenSymbol = result.Swap.Token0Symbol
		result.SoldTokenAddress = result.Swap.Token1Address
		result.SoldTokenSymbol = result.Swap.Token1Symbol
	case sign1 < 0 && sign0 > 0:
		result.BoughtTokenAddress = result.Swap.Token1Address
		result.BoughtTokenSymbol = result.Swap.Token1Symbol
		result.SoldTokenAddress = result.Swap.Token0Address
		result.SoldTokenSymbol = result.Swap.Token0Symbol
	}
}

func priceSource(price0, price1 Price, fresh0, fresh1 bool) string {
	if fresh0 {
		return price0.Source
	}
	if fresh1 {
		return price1.Source
	}
	return ""
}

func formatDecimal(value *big.Rat) string {
	if value == nil {
		return ""
	}
	formatted := value.FloatString(18)
	formatted = strings.TrimRight(formatted, "0")
	formatted = strings.TrimRight(formatted, ".")
	if formatted == "" {
		return "0"
	}
	return formatted
}
