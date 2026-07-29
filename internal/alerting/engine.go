package alerting

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"sort"
	"strings"
	"time"
)

type Engine struct {
	source             CandidateStore
	outbox             Outbox
	logger             *slog.Logger
	quoteSymbols       map[string]struct{}
	criticalUSD        *big.Rat
	smartScoreMin      *big.Rat
	smartConfidenceMin *big.Rat
	smartTradeMinUSD   *big.Rat
	smartScoreVersion  string
	lookback           time.Duration
	batchSize          int
	pollInterval       time.Duration
	cursor             Cursor
}

type EngineConfig struct {
	QuoteSymbols       []string
	CriticalUSD        string
	SmartScoreVersion  string
	SmartScoreMin      string
	SmartConfidenceMin string
	SmartTradeMinUSD   string
	Lookback           time.Duration
	BatchSize          int
	PollInterval       time.Duration
}

func NewEngine(
	source CandidateStore,
	outbox Outbox,
	logger *slog.Logger,
	config EngineConfig,
) (*Engine, error) {
	if source == nil || outbox == nil {
		return nil, fmt.Errorf("alert candidate source and outbox are required")
	}
	if logger == nil {
		return nil, fmt.Errorf("alert logger is required")
	}
	threshold, ok := new(big.Rat).SetString(config.CriticalUSD)
	if !ok || threshold.Sign() < 0 {
		return nil, fmt.Errorf("critical alert threshold must be a non-negative decimal")
	}
	scoreThreshold, ok := new(big.Rat).SetString(config.SmartScoreMin)
	if !ok || scoreThreshold.Sign() < 0 || scoreThreshold.Cmp(big.NewRat(100, 1)) > 0 {
		return nil, fmt.Errorf("smart score threshold must be between 0 and 100")
	}
	confidenceThreshold, ok := new(big.Rat).SetString(config.SmartConfidenceMin)
	if !ok || confidenceThreshold.Sign() < 0 || confidenceThreshold.Cmp(big.NewRat(1, 1)) > 0 {
		return nil, fmt.Errorf("smart confidence threshold must be between 0 and 1")
	}
	tradeThreshold, ok := new(big.Rat).SetString(config.SmartTradeMinUSD)
	if !ok || tradeThreshold.Sign() < 0 {
		return nil, fmt.Errorf("smart trade threshold must be a non-negative decimal")
	}
	smartScoreVersion := strings.TrimSpace(config.SmartScoreVersion)
	if smartScoreVersion == "" {
		return nil, fmt.Errorf("smart score version is required")
	}
	if config.Lookback <= 0 || config.BatchSize <= 0 || config.PollInterval <= 0 {
		return nil, fmt.Errorf("alert lookback, batch size, and poll interval must be positive")
	}
	quotes := make(map[string]struct{}, len(config.QuoteSymbols))
	for _, symbol := range config.QuoteSymbols {
		if symbol = strings.ToUpper(strings.TrimSpace(symbol)); symbol != "" {
			quotes[symbol] = struct{}{}
		}
	}
	return &Engine{
		source:             source,
		outbox:             outbox,
		logger:             logger,
		quoteSymbols:       quotes,
		criticalUSD:        threshold,
		smartScoreMin:      scoreThreshold,
		smartConfidenceMin: confidenceThreshold,
		smartTradeMinUSD:   tradeThreshold,
		smartScoreVersion:  smartScoreVersion,
		lookback:           config.Lookback,
		batchSize:          config.BatchSize,
		pollInterval:       config.PollInterval,
	}, nil
}

func (e *Engine) Run(ctx context.Context) error {
	for {
		scanned, inserted, err := e.process(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			e.logger.Error("alert engine cycle failed", "error", err)
		} else if inserted > 0 {
			e.logger.Info(
				"alert engine cycle complete",
				"scanned", scanned,
				"inserted", inserted,
			)
		} else if scanned > 0 {
			e.logger.Debug(
				"alert engine cycle produced no new alerts",
				"scanned", scanned,
			)
		}
		if scanned < e.batchSize {
			if err := wait(ctx, e.pollInterval); err != nil {
				return nil
			}
		} else if ctx.Err() != nil {
			return nil
		}
	}
}

func (e *Engine) process(ctx context.Context) (int, int, error) {
	candidates, err := e.source.RealtimeAlertCandidates(
		ctx,
		e.lookback,
		e.cursor,
		e.smartScoreVersion,
		e.batchSize,
	)
	if err != nil {
		return 0, 0, fmt.Errorf("list realtime alert candidates: %w", err)
	}
	alerts := make([]Alert, 0, len(candidates)*2)
	for _, candidate := range candidates {
		candidateAlerts, err := e.buildAlerts(candidate)
		if err != nil {
			e.logger.Warn(
				"skip invalid alert candidate",
				"event_id", candidate.EventID,
				"error", err,
			)
			continue
		}
		alerts = append(alerts, candidateAlerts...)
	}
	inserted, err := e.outbox.InsertAlerts(ctx, alerts)
	if err != nil {
		return 0, 0, fmt.Errorf("insert alert outbox records: %w", err)
	}
	if len(candidates) > 0 {
		last := candidates[len(candidates)-1]
		e.cursor = Cursor{ObservedAt: last.ObservedAt, EventID: last.EventID}
	}
	return len(candidates), inserted, nil
}

func (e *Engine) buildAlert(candidate Candidate) (Alert, error) {
	return e.buildTypedAlert(candidate, false)
}

func (e *Engine) buildAlerts(candidate Candidate) ([]Alert, error) {
	value, ok := new(big.Rat).SetString(candidate.TradeValueUSDRaw)
	if !ok {
		return nil, fmt.Errorf("invalid trade value %q", candidate.TradeValueUSDRaw)
	}
	alerts := make([]Alert, 0, 2)
	if candidate.IsLargeTrade {
		alert, err := e.buildTypedAlert(candidate, false)
		if err != nil {
			return nil, err
		}
		alerts = append(alerts, alert)
	}
	if e.isSmartMoney(candidate, value) {
		alert, err := e.buildTypedAlert(candidate, true)
		if err != nil {
			return nil, err
		}
		alerts = append(alerts, alert)
	}
	return alerts, nil
}

func (e *Engine) BuildAlerts(candidate Candidate) ([]Alert, error) {
	return e.buildAlerts(candidate)
}

func (e *Engine) isSmartMoney(candidate Candidate, value *big.Rat) bool {
	if candidate.SmartScoreVersion != e.smartScoreVersion ||
		value.Cmp(e.smartTradeMinUSD) < 0 {
		return false
	}
	score, ok := new(big.Rat).SetString(candidate.SmartScoreRaw)
	if !ok || score.Cmp(e.smartScoreMin) < 0 {
		return false
	}
	confidence, ok := new(big.Rat).SetString(candidate.SmartConfidenceRaw)
	return ok && confidence.Cmp(e.smartConfidenceMin) >= 0
}

func (e *Engine) buildTypedAlert(candidate Candidate, smartMoney bool) (Alert, error) {
	value, ok := new(big.Rat).SetString(candidate.TradeValueUSDRaw)
	if !ok {
		return Alert{}, fmt.Errorf("invalid trade value %q", candidate.TradeValueUSDRaw)
	}
	alertType, tokenAddress, tokenSymbol, risk := e.classifyTrade(candidate)
	severity := "medium"
	if risk.IsHoneypot || risk.HasBlackMethod {
		severity = "critical"
	} else if value.Cmp(e.criticalUSD) >= 0 || risk.HasMintMethod || risk.IsProxy {
		severity = "high"
	}
	if smartMoney {
		alertType = strings.Replace(alertType, "large_", "smart_money_", 1)
		if severity == "medium" {
			severity = "high"
		}
	}
	payload, err := json.Marshal(map[string]any{
		"event_id":             candidate.EventID,
		"valuation_version":    candidate.ValuationVersion,
		"wallet_address":       candidate.WalletAddress,
		"attribution_method":   candidate.AttributionMethod,
		"block_time":           candidate.BlockTime,
		"pool_address":         candidate.PoolAddress,
		"protocol":             candidate.Protocol,
		"protocol_version":     candidate.ProtocolVersion,
		"trade_value_usd_raw":  candidate.TradeValueUSDRaw,
		"bought_token_address": candidate.BoughtTokenAddress,
		"bought_token_symbol":  candidate.BoughtTokenSymbol,
		"sold_token_address":   candidate.SoldTokenAddress,
		"sold_token_symbol":    candidate.SoldTokenSymbol,
		"target_risk":          risk,
		"valued_at":            candidate.ValuedAt,
		"observed_at":          candidate.ObservedAt,
		"is_large_trade":       candidate.IsLargeTrade,
		"smart_score": map[string]any{
			"version":           candidate.SmartScoreVersion,
			"score_raw":         candidate.SmartScoreRaw,
			"grade":             candidate.SmartScoreGrade,
			"confidence_raw":    candidate.SmartConfidenceRaw,
			"source_updated_at": candidate.SmartScoreSourceAt,
			"calculated_at":     candidate.SmartScoreAt,
		},
	})
	if err != nil {
		return Alert{}, fmt.Errorf("encode alert payload: %w", err)
	}
	return Alert{
		Key: fmt.Sprintf(
			"%s:%s:%s:%s",
			candidate.ValuationVersion,
			candidate.EventID,
			alertType,
			strings.ToLower(tokenAddress),
		),
		Type:            alertType,
		Severity:        severity,
		ChainID:         candidate.ChainID,
		BlockNumber:     candidate.BlockNumber,
		TransactionHash: candidate.TransactionHash,
		TokenAddress:    tokenAddress,
		TokenSymbol:     tokenSymbol,
		Title:           alertTitle(alertType, tokenSymbol, candidate.TradeValueUSDRaw),
		Payload:         payload,
	}, nil
}

func (e *Engine) classifyTrade(candidate Candidate) (string, string, string, RiskFlags) {
	boughtIsQuote := e.isQuote(candidate.BoughtTokenSymbol)
	soldIsQuote := e.isQuote(candidate.SoldTokenSymbol)
	switch {
	case !boughtIsQuote && soldIsQuote:
		return "large_buy",
			candidate.BoughtTokenAddress,
			candidate.BoughtTokenSymbol,
			candidate.BoughtRisk
	case boughtIsQuote && !soldIsQuote:
		return "large_sell",
			candidate.SoldTokenAddress,
			candidate.SoldTokenSymbol,
			candidate.SoldRisk
	default:
		return "large_swap",
			candidate.BoughtTokenAddress,
			candidate.BoughtTokenSymbol,
			candidate.BoughtRisk
	}
}

func (e *Engine) isQuote(symbol string) bool {
	_, exists := e.quoteSymbols[strings.ToUpper(strings.TrimSpace(symbol))]
	return exists
}

func alertTitle(alertType, symbol, value string) string {
	label := strings.ReplaceAll(alertType, "_", " ")
	if symbol == "" {
		symbol = "unknown token"
	}
	return fmt.Sprintf("%s: %s ($%s)", label, symbol, value)
}

func SortedQuoteSymbols(raw string) []string {
	values := strings.Split(raw, ",")
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{})
	for _, value := range values {
		value = strings.ToUpper(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
