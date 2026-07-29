package walletanalytics

import (
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"time"
)

type Calculator struct {
	quoteSymbols map[string]struct{}
	maxPriceAge  time.Duration
}

type tokenState struct {
	address             string
	symbol              string
	quantity            *big.Rat
	cost                *big.Rat
	boughtAmount        *big.Rat
	soldAmount          *big.Rat
	totalBuyCost        *big.Rat
	totalSellIncome     *big.Rat
	realizedProfit      *big.Rat
	unmatchedSellAmount *big.Rat
	unmatchedSellUSD    *big.Rat
	buyCount            uint64
	sellCount           uint64
	winningSellCount    uint64
	coveredSellCount    uint64
	unmatchedSellCount  uint64
	firstTradedAt       time.Time
	lastTradedAt        time.Time
	sourceUpdatedAt     time.Time
}

func NewCalculator(quoteSymbols []string, maxPriceAge time.Duration) (*Calculator, error) {
	if maxPriceAge <= 0 {
		return nil, fmt.Errorf("wallet analytics maximum price age must be positive")
	}
	quotes := make(map[string]struct{}, len(quoteSymbols))
	for _, symbol := range quoteSymbols {
		if symbol = strings.ToUpper(strings.TrimSpace(symbol)); symbol != "" {
			quotes[symbol] = struct{}{}
		}
	}
	if len(quotes) == 0 {
		return nil, fmt.Errorf("wallet analytics quote symbols are required")
	}
	return &Calculator{quoteSymbols: quotes, maxPriceAge: maxPriceAge}, nil
}

func (c *Calculator) Calculate(input Input, calculatedAt time.Time) (Result, error) {
	if input.ChainID == 0 || strings.TrimSpace(input.WalletAddress) == "" {
		return Result{}, fmt.Errorf("wallet analytics chain and address are required")
	}
	if calculatedAt.IsZero() {
		calculatedAt = time.Now().UTC()
	}
	trades := append([]Trade(nil), input.Trades...)
	sort.SliceStable(trades, func(i, j int) bool {
		if trades[i].BlockTime.Equal(trades[j].BlockTime) {
			return trades[i].EventID < trades[j].EventID
		}
		return trades[i].BlockTime.Before(trades[j].BlockTime)
	})

	states := make(map[string]*tokenState)
	activeDays := make(map[string]struct{})
	partialValuations := uint64(0)
	sourceUpdatedAt := time.Time{}
	for _, trade := range trades {
		if trade.ValuationStatus != "valued" {
			partialValuations++
		}
		if trade.GeneratedAt.After(sourceUpdatedAt) {
			sourceUpdatedAt = trade.GeneratedAt
		}
		if !trade.BlockTime.IsZero() {
			activeDays[trade.BlockTime.UTC().Format("2006-01-02")] = struct{}{}
		}
		value, err := positiveRat(trade.TradeValueUSDRaw)
		if err != nil {
			continue
		}
		boughtAmount, err := positiveRat(trade.BoughtAmountRaw)
		if err == nil {
			state := getTokenState(
				states,
				trade.BoughtTokenAddress,
				trade.BoughtTokenSymbol,
			)
			state.applyBuy(boughtAmount, value, trade)
		}
		soldAmount, err := positiveRat(trade.SoldAmountRaw)
		if err == nil {
			state := getTokenState(
				states,
				trade.SoldTokenAddress,
				trade.SoldTokenSymbol,
			)
			state.applySell(soldAmount, value, trade)
		}
	}

	addresses := make([]string, 0, len(states))
	for address := range states {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)

	result := Result{Tokens: make([]TokenPnL, 0, len(addresses))}
	totalRealized := new(big.Rat)
	totalUnrealized := new(big.Rat)
	totalInvested := new(big.Rat)
	closedSells := uint64(0)
	winningSells := uint64(0)
	riskyTokens := uint64(0)
	unmatchedSells := uint64(0)
	missingPrices := uint64(0)
	nonQuoteTokens := uint64(0)
	for _, address := range addresses {
		state := states[address]
		token := c.tokenResult(
			input,
			state,
			calculatedAt,
		)
		result.Tokens = append(result.Tokens, token)
		if token.IsQuoteToken {
			continue
		}
		nonQuoteTokens++
		addDecimal(totalRealized, token.RealizedProfitUSDRaw)
		addDecimal(totalInvested, token.TotalBuyCostUSDRaw)
		if token.UnrealizedProfitUSDRaw != "" {
			addDecimal(totalUnrealized, token.UnrealizedProfitUSDRaw)
		}
		closedSells += state.coveredSellCount
		winningSells += token.WinningSellCount
		if token.Risk.Risky() {
			riskyTokens++
		}
		unmatchedSells += state.unmatchedSellCount
		if state.quantity.Sign() > 0 && token.CurrentPriceUSDRaw == "" {
			missingPrices++
		}
	}
	totalProfit := new(big.Rat).Add(totalRealized, totalUnrealized)
	roi := new(big.Rat)
	if totalInvested.Sign() > 0 {
		roi.Quo(totalProfit, totalInvested)
	}
	winRate := new(big.Rat)
	if closedSells > 0 {
		winRate.SetFrac(
			new(big.Int).SetUint64(winningSells),
			new(big.Int).SetUint64(closedSells),
		)
	}
	score := buildScore(
		len(trades),
		len(activeDays),
		int(nonQuoteTokens),
		closedSells,
		riskyTokens,
		unmatchedSells,
		missingPrices,
		partialValuations,
		input.TransferInCount,
		input.TransferOutCount,
		ratFloat(roi),
		ratFloat(winRate),
	)
	result.Score = WalletScore{
		ChainID:                   input.ChainID,
		WalletAddress:             input.WalletAddress,
		AnalyticsVersion:          Version,
		RealizedProfitUSDRaw:      formatRat(totalRealized),
		UnrealizedProfitUSDRaw:    formatRat(totalUnrealized),
		TotalProfitUSDRaw:         formatRat(totalProfit),
		TotalInvestedUSDRaw:       formatRat(totalInvested),
		ROIRaw:                    formatRat(roi),
		WinRateRaw:                formatRat(winRate),
		SmartScoreRaw:             formatScore(score.total),
		SmartScoreGrade:           scoreGrade(score.total),
		PerformanceScoreRaw:       formatScore(score.performance),
		WinRateScoreRaw:           formatScore(score.winRate),
		TrackRecordScoreRaw:       formatScore(score.trackRecord),
		ActivityScoreRaw:          formatScore(score.activity),
		RiskScoreRaw:              formatScore(score.risk),
		ConfidenceRaw:             formatScore(score.confidence),
		TradeCount:                uint64(len(trades)),
		ClosedSellCount:           closedSells,
		WinningSellCount:          winningSells,
		ActiveDays:                uint64(len(activeDays)),
		UniqueNonQuoteTokens:      nonQuoteTokens,
		RiskyTokenCount:           riskyTokens,
		UnmatchedSellCount:        unmatchedSells,
		MissingPricePositionCount: missingPrices,
		PartialValuationCount:     partialValuations,
		TransferInCount:           input.TransferInCount,
		TransferOutCount:          input.TransferOutCount,
		HistoryIncomplete: unmatchedSells > 0 ||
			input.TransferInCount > 0 ||
			input.TransferOutCount > 0,
		SourceUpdatedAt:   sourceUpdatedAt,
		SourceUpdatedAtMS: uint64(sourceUpdatedAt.UnixMilli()),
		CalculatedAt:      calculatedAt,
	}
	return result, nil
}

func (c *Calculator) tokenResult(
	input Input,
	state *tokenState,
	calculatedAt time.Time,
) TokenPnL {
	price := input.Prices[state.address]
	risk := input.Risks[state.address]
	isQuote := c.isQuote(state.symbol)
	averageCost := new(big.Rat)
	if state.quantity.Sign() > 0 {
		averageCost.Quo(state.cost, state.quantity)
	}
	currentValue := new(big.Rat)
	unrealized := new(big.Rat)
	priceAvailable := false
	if state.quantity.Sign() == 0 {
		priceAvailable = true
	} else if parsedPrice, err := positiveRat(price.Raw); err == nil &&
		!price.UpdatedAt.IsZero() &&
		absoluteDuration(calculatedAt.Sub(price.UpdatedAt)) <= c.maxPriceAge {
		currentValue.Mul(state.quantity, parsedPrice)
		unrealized.Sub(currentValue, state.cost)
		priceAvailable = true
	}
	totalProfit := new(big.Rat).Set(state.realizedProfit)
	if priceAvailable {
		totalProfit.Add(totalProfit, unrealized)
	}
	quality := "complete"
	if state.unmatchedSellAmount.Sign() > 0 {
		quality = "incomplete_history"
	} else if !priceAvailable {
		quality = "missing_price"
	}
	token := TokenPnL{
		ChainID:                input.ChainID,
		WalletAddress:          input.WalletAddress,
		TokenAddress:           state.address,
		TokenSymbol:            state.symbol,
		AnalyticsVersion:       Version,
		IsQuoteToken:           isQuote,
		BoughtAmountRaw:        formatRat(state.boughtAmount),
		SoldAmountRaw:          formatRat(state.soldAmount),
		RemainingAmountRaw:     formatRat(state.quantity),
		TotalBuyCostUSDRaw:     formatRat(state.totalBuyCost),
		TotalSellIncomeUSDRaw:  formatRat(state.totalSellIncome),
		RemainingCostUSDRaw:    formatRat(state.cost),
		RealizedProfitUSDRaw:   formatRat(state.realizedProfit),
		TotalProfitUSDRaw:      formatRat(totalProfit),
		AverageCostUSDRaw:      formatRat(averageCost),
		BuyCount:               state.buyCount,
		SellCount:              state.sellCount,
		WinningSellCount:       state.winningSellCount,
		UnmatchedSellAmountRaw: formatRat(state.unmatchedSellAmount),
		UnmatchedSellUSDRaw:    formatRat(state.unmatchedSellUSD),
		Risk:                   risk,
		DataQuality:            quality,
		FirstTradedAt:          state.firstTradedAt,
		LastTradedAt:           state.lastTradedAt,
		SourceUpdatedAt:        state.sourceUpdatedAt,
		CalculatedAt:           calculatedAt,
	}
	if priceAvailable {
		token.UnrealizedProfitUSDRaw = formatRat(unrealized)
		token.CurrentValueUSDRaw = formatRat(currentValue)
	}
	if state.quantity.Sign() == 0 {
		token.CurrentPriceUSDRaw = price.Raw
		token.PriceUpdatedAt = price.UpdatedAt
	} else if priceAvailable {
		token.CurrentPriceUSDRaw = price.Raw
		token.PriceUpdatedAt = price.UpdatedAt
	}
	return token
}

func (c *Calculator) isQuote(symbol string) bool {
	_, exists := c.quoteSymbols[strings.ToUpper(strings.TrimSpace(symbol))]
	return exists
}

func getTokenState(
	states map[string]*tokenState,
	address string,
	symbol string,
) *tokenState {
	address = strings.ToLower(strings.TrimSpace(address))
	if state, exists := states[address]; exists {
		if state.symbol == "" {
			state.symbol = symbol
		}
		return state
	}
	state := &tokenState{
		address:             address,
		symbol:              symbol,
		quantity:            new(big.Rat),
		cost:                new(big.Rat),
		boughtAmount:        new(big.Rat),
		soldAmount:          new(big.Rat),
		totalBuyCost:        new(big.Rat),
		totalSellIncome:     new(big.Rat),
		realizedProfit:      new(big.Rat),
		unmatchedSellAmount: new(big.Rat),
		unmatchedSellUSD:    new(big.Rat),
	}
	states[address] = state
	return state
}

func (s *tokenState) applyBuy(amount, value *big.Rat, trade Trade) {
	s.quantity.Add(s.quantity, amount)
	s.cost.Add(s.cost, value)
	s.boughtAmount.Add(s.boughtAmount, amount)
	s.totalBuyCost.Add(s.totalBuyCost, value)
	s.buyCount++
	s.observe(trade)
}

func (s *tokenState) applySell(amount, proceeds *big.Rat, trade Trade) {
	s.soldAmount.Add(s.soldAmount, amount)
	s.totalSellIncome.Add(s.totalSellIncome, proceeds)
	s.sellCount++
	covered := minimumRat(amount, s.quantity)
	coveredProceeds := proportional(proceeds, covered, amount)
	removedCost := new(big.Rat)
	if s.quantity.Sign() > 0 && covered.Sign() > 0 {
		removedCost.Mul(new(big.Rat).Quo(s.cost, s.quantity), covered)
	}
	eventProfit := new(big.Rat).Sub(coveredProceeds, removedCost)
	s.realizedProfit.Add(s.realizedProfit, eventProfit)
	if covered.Sign() > 0 && eventProfit.Sign() > 0 {
		s.winningSellCount++
	}
	if covered.Sign() > 0 {
		s.coveredSellCount++
	}
	s.quantity.Sub(s.quantity, covered)
	s.cost.Sub(s.cost, removedCost)
	if s.quantity.Sign() == 0 {
		s.cost.SetInt64(0)
	}
	unmatched := new(big.Rat).Sub(amount, covered)
	if unmatched.Sign() > 0 {
		s.unmatchedSellAmount.Add(s.unmatchedSellAmount, unmatched)
		s.unmatchedSellUSD.Add(
			s.unmatchedSellUSD,
			proportional(proceeds, unmatched, amount),
		)
		s.unmatchedSellCount++
	}
	s.observe(trade)
}

func (s *tokenState) observe(trade Trade) {
	if s.firstTradedAt.IsZero() || trade.BlockTime.Before(s.firstTradedAt) {
		s.firstTradedAt = trade.BlockTime
	}
	if trade.BlockTime.After(s.lastTradedAt) {
		s.lastTradedAt = trade.BlockTime
	}
	if trade.GeneratedAt.After(s.sourceUpdatedAt) {
		s.sourceUpdatedAt = trade.GeneratedAt
	}
}

type scoreComponents struct {
	total       float64
	performance float64
	winRate     float64
	trackRecord float64
	activity    float64
	risk        float64
	confidence  float64
}

func buildScore(
	trades int,
	activeDays int,
	nonQuoteTokens int,
	closedSells uint64,
	riskyTokens uint64,
	unmatchedSells uint64,
	missingPrices uint64,
	partialValuations uint64,
	transferIn uint64,
	transferOut uint64,
	roi float64,
	winRate float64,
) scoreComponents {
	components := scoreComponents{}
	components.performance = clamp((roi+0.5)/2.5, 0, 1) * 35
	components.winRate = clamp(winRate, 0, 1) * 25
	components.trackRecord = clamp(float64(closedSells)/20, 0, 1) * 15
	components.activity =
		clamp(float64(activeDays)/30, 0, 1)*10 +
			clamp(float64(nonQuoteTokens)/10, 0, 1)*5
	riskRatio := 0.0
	if nonQuoteTokens > 0 {
		riskRatio = float64(riskyTokens) / float64(nonQuoteTokens)
	}
	components.risk = (1 - clamp(riskRatio, 0, 1)) * 10
	confidence := 0.5 + 0.5*clamp(float64(trades)/20, 0, 1)
	if transferIn+transferOut > 0 {
		confidence *= 0.85
	}
	if trades > 0 {
		confidence *= math.Max(
			0.5,
			1-clamp(float64(unmatchedSells)/float64(trades), 0, 1),
		)
		confidence *= math.Max(
			0.7,
			1-clamp(float64(partialValuations)/float64(trades), 0, 1)*0.3,
		)
	}
	if nonQuoteTokens > 0 {
		confidence *= math.Max(
			0.6,
			1-clamp(float64(missingPrices)/float64(nonQuoteTokens), 0, 1)*0.4,
		)
	}
	components.confidence = clamp(confidence, 0, 1)
	base := components.performance +
		components.winRate +
		components.trackRecord +
		components.activity +
		components.risk
	components.total = clamp(base*(0.7+0.3*components.confidence), 0, 100)
	return components
}

func scoreGrade(score float64) string {
	switch {
	case score >= 80:
		return "A"
	case score >= 65:
		return "B"
	case score >= 50:
		return "C"
	case score >= 35:
		return "D"
	default:
		return "E"
	}
}

func positiveRat(raw string) (*big.Rat, error) {
	value, ok := new(big.Rat).SetString(strings.TrimSpace(raw))
	if !ok || value.Sign() <= 0 {
		return nil, fmt.Errorf("invalid positive decimal %q", raw)
	}
	return value, nil
}

func addDecimal(target *big.Rat, raw string) {
	if value, ok := new(big.Rat).SetString(strings.TrimSpace(raw)); ok {
		target.Add(target, value)
	}
}

func minimumRat(left, right *big.Rat) *big.Rat {
	if left.Cmp(right) <= 0 {
		return new(big.Rat).Set(left)
	}
	return new(big.Rat).Set(right)
}

func proportional(total, part, whole *big.Rat) *big.Rat {
	if whole.Sign() == 0 {
		return new(big.Rat)
	}
	return new(big.Rat).Mul(total, new(big.Rat).Quo(part, whole))
}

func formatRat(value *big.Rat) string {
	if value == nil {
		return ""
	}
	formatted := value.FloatString(18)
	formatted = strings.TrimRight(formatted, "0")
	formatted = strings.TrimRight(formatted, ".")
	if formatted == "" || formatted == "-0" {
		return "0"
	}
	return formatted
}

func ratFloat(value *big.Rat) float64 {
	result, _ := value.Float64()
	return result
}

func formatScore(value float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.6f", value), "0"), ".")
}

func clamp(value, minimum, maximum float64) float64 {
	return math.Min(maximum, math.Max(minimum, value))
}

func absoluteDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}
