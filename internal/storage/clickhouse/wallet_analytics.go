package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/basewatch/base-analytics/internal/walletanalytics"
)

func (s *EventStore) WalletAnalysisCandidates(
	ctx context.Context,
	analyticsVersion string,
	limit int,
) ([]walletanalytics.Candidate, error) {
	rows, err := s.conn.Query(ctx, `
		WITH
			wallet_sources AS (
				SELECT
					chain_id,
					wallet_address,
					max(generated_at) AS source_updated_at,
					toUInt64(toUnixTimestamp64Milli(max(generated_at))) AS source_updated_at_ms
				FROM wallet_swap_activities FINAL
				GROUP BY chain_id, wallet_address
			),
			latest_scores AS (
				SELECT
					chain_id,
					wallet_address,
					argMax(source_updated_at_ms, calculated_at) AS analyzed_source_updated_at_ms
				FROM wallet_smart_score_snapshots
				WHERE analytics_version = ?
				GROUP BY chain_id, wallet_address
			)
		SELECT w.chain_id, w.wallet_address
		FROM wallet_sources AS w
		LEFT JOIN latest_scores AS s
			ON s.chain_id = w.chain_id AND s.wallet_address = w.wallet_address
		WHERE s.wallet_address = ''
		   OR s.analyzed_source_updated_at_ms < w.source_updated_at_ms
		ORDER BY w.source_updated_at, w.wallet_address
		LIMIT ?`,
		analyticsVersion,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query wallet analysis candidates: %w", err)
	}
	defer rows.Close()
	candidates := make([]walletanalytics.Candidate, 0, limit)
	for rows.Next() {
		var candidate walletanalytics.Candidate
		if err := rows.Scan(&candidate.ChainID, &candidate.WalletAddress); err != nil {
			return nil, fmt.Errorf("scan wallet analysis candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wallet analysis candidates: %w", err)
	}
	return candidates, nil
}

func (s *EventStore) LoadWalletAnalysis(
	ctx context.Context,
	candidate walletanalytics.Candidate,
) (walletanalytics.Input, error) {
	input := walletanalytics.Input{
		ChainID:       candidate.ChainID,
		WalletAddress: candidate.WalletAddress,
		Prices:        make(map[string]walletanalytics.Price),
		Risks:         make(map[string]walletanalytics.Risk),
	}
	rows, err := s.conn.Query(ctx, `
		SELECT
			event_id, block_time, bought_token_address, bought_token_symbol,
			bought_token_amount_raw, sold_token_address, sold_token_symbol,
			sold_token_amount_raw, trade_value_usd_raw, valuation_status,
			generated_at
		FROM wallet_swap_activities FINAL
		WHERE chain_id = ? AND wallet_address = ?
		ORDER BY block_time, transaction_hash, log_index, event_id`,
		candidate.ChainID,
		candidate.WalletAddress,
	)
	if err != nil {
		return walletanalytics.Input{}, fmt.Errorf("query wallet analysis trades: %w", err)
	}
	for rows.Next() {
		var trade walletanalytics.Trade
		if err := rows.Scan(
			&trade.EventID,
			&trade.BlockTime,
			&trade.BoughtTokenAddress,
			&trade.BoughtTokenSymbol,
			&trade.BoughtAmountRaw,
			&trade.SoldTokenAddress,
			&trade.SoldTokenSymbol,
			&trade.SoldAmountRaw,
			&trade.TradeValueUSDRaw,
			&trade.ValuationStatus,
			&trade.GeneratedAt,
		); err != nil {
			rows.Close()
			return walletanalytics.Input{}, fmt.Errorf("scan wallet analysis trade: %w", err)
		}
		input.Trades = append(input.Trades, trade)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return walletanalytics.Input{}, fmt.Errorf("iterate wallet analysis trades: %w", err)
	}
	rows.Close()

	marketRows, err := s.conn.Query(ctx, `
		WITH
			wallet_tokens AS (
				SELECT arrayJoin([bought_token_address, sold_token_address]) AS token_address
				FROM wallet_swap_activities FINAL
				WHERE chain_id = ? AND wallet_address = ?
				GROUP BY token_address
			),
			latest_market AS (
				SELECT
					token_address,
					argMax(price_usd_raw, fetched_at) AS latest_price_usd_raw,
					max(fetched_at) AS latest_price_updated_at
				FROM token_market_snapshots
				WHERE chain_id = ?
				  AND token_address IN (SELECT token_address FROM wallet_tokens)
				GROUP BY token_address
			),
			latest_risk AS (
				SELECT
					token_address,
					argMax(ifNull(is_honeypot, 0), fetched_at) AS latest_is_honeypot,
					argMax(ifNull(has_mint_method, 0), fetched_at) AS latest_has_mint_method,
					argMax(ifNull(has_black_method, 0), fetched_at) AS latest_has_black_method,
					argMax(ifNull(is_proxy, 0), fetched_at) AS latest_is_proxy
				FROM token_risk_snapshots
				WHERE chain_id = ?
				  AND token_address IN (SELECT token_address FROM wallet_tokens)
				GROUP BY token_address
			)
		SELECT
			t.token_address,
			m.latest_price_usd_raw,
			m.latest_price_updated_at,
			r.latest_is_honeypot,
			r.latest_has_mint_method,
			r.latest_has_black_method,
			r.latest_is_proxy
		FROM wallet_tokens AS t
		LEFT JOIN latest_market AS m ON m.token_address = t.token_address
		LEFT JOIN latest_risk AS r ON r.token_address = t.token_address`,
		candidate.ChainID,
		candidate.WalletAddress,
		candidate.ChainID,
		candidate.ChainID,
	)
	if err != nil {
		return walletanalytics.Input{}, fmt.Errorf("query wallet token context: %w", err)
	}
	for marketRows.Next() {
		var address, priceRaw string
		var priceUpdatedAt time.Time
		var honeypot, mint, black, proxy uint8
		if err := marketRows.Scan(
			&address,
			&priceRaw,
			&priceUpdatedAt,
			&honeypot,
			&mint,
			&black,
			&proxy,
		); err != nil {
			marketRows.Close()
			return walletanalytics.Input{}, fmt.Errorf("scan wallet token context: %w", err)
		}
		input.Prices[address] = walletanalytics.Price{
			Raw:       priceRaw,
			UpdatedAt: priceUpdatedAt,
		}
		input.Risks[address] = walletanalytics.Risk{
			IsHoneypot:     honeypot == 1,
			HasMintMethod:  mint == 1,
			HasBlackMethod: black == 1,
			IsProxy:        proxy == 1,
		}
	}
	if err := marketRows.Err(); err != nil {
		marketRows.Close()
		return walletanalytics.Input{}, fmt.Errorf("iterate wallet token context: %w", err)
	}
	marketRows.Close()

	err = s.conn.QueryRow(ctx, `
		SELECT
			countIf(to_address = ?),
			countIf(from_address = ?)
		FROM erc20_transfers FINAL
		WHERE chain_id = ?
		  AND is_canonical = 1
		  AND (from_address = ? OR to_address = ?)`,
		candidate.WalletAddress,
		candidate.WalletAddress,
		candidate.ChainID,
		candidate.WalletAddress,
		candidate.WalletAddress,
	).Scan(&input.TransferInCount, &input.TransferOutCount)
	if err != nil {
		return walletanalytics.Input{}, fmt.Errorf("query wallet analysis transfers: %w", err)
	}
	return input, nil
}

func (s *EventStore) InsertWalletAnalysis(
	ctx context.Context,
	result walletanalytics.Result,
) error {
	if len(result.Tokens) > 0 {
		batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO wallet_token_pnl_snapshots (
			chain_id, wallet_address, token_address, token_symbol,
			analytics_version, is_quote_token, bought_amount_raw,
			sold_amount_raw, remaining_amount_raw, total_buy_cost_usd_raw,
			total_sell_income_usd_raw, remaining_cost_usd_raw,
			realized_profit_usd_raw, unrealized_profit_usd_raw,
			total_profit_usd_raw, current_value_usd_raw, average_cost_usd_raw,
			current_price_usd_raw, buy_count, sell_count, winning_sell_count,
			unmatched_sell_amount_raw, unmatched_sell_usd_raw, is_honeypot,
			has_mint_method, has_black_method, is_proxy, data_quality,
			first_traded_at, last_traded_at, price_updated_at,
			source_updated_at, calculated_at
		)`)
		if err != nil {
			return fmt.Errorf("prepare wallet token PnL batch: %w", err)
		}
		for _, token := range result.Tokens {
			if err := batch.Append(
				token.ChainID,
				token.WalletAddress,
				token.TokenAddress,
				token.TokenSymbol,
				token.AnalyticsVersion,
				boolToUInt8(token.IsQuoteToken),
				token.BoughtAmountRaw,
				token.SoldAmountRaw,
				token.RemainingAmountRaw,
				token.TotalBuyCostUSDRaw,
				token.TotalSellIncomeUSDRaw,
				token.RemainingCostUSDRaw,
				token.RealizedProfitUSDRaw,
				token.UnrealizedProfitUSDRaw,
				token.TotalProfitUSDRaw,
				token.CurrentValueUSDRaw,
				token.AverageCostUSDRaw,
				token.CurrentPriceUSDRaw,
				token.BuyCount,
				token.SellCount,
				token.WinningSellCount,
				token.UnmatchedSellAmountRaw,
				token.UnmatchedSellUSDRaw,
				boolToUInt8(token.Risk.IsHoneypot),
				boolToUInt8(token.Risk.HasMintMethod),
				boolToUInt8(token.Risk.HasBlackMethod),
				boolToUInt8(token.Risk.IsProxy),
				token.DataQuality,
				safeDateTime(token.FirstTradedAt),
				safeDateTime(token.LastTradedAt),
				safeDateTime(token.PriceUpdatedAt),
				safeDateTime(token.SourceUpdatedAt),
				token.CalculatedAt,
			); err != nil {
				return fmt.Errorf("append wallet token PnL %s: %w", token.TokenAddress, err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("send wallet token PnL batch: %w", err)
		}
	}
	score := result.Score
	err := s.conn.Exec(ctx, `INSERT INTO wallet_smart_score_snapshots (
		chain_id, wallet_address, analytics_version, realized_profit_usd_raw,
		unrealized_profit_usd_raw, total_profit_usd_raw,
		total_invested_usd_raw, roi_raw, win_rate_raw, smart_score_raw,
		smart_score_grade, performance_score_raw, win_rate_score_raw,
		track_record_score_raw, activity_score_raw, risk_score_raw,
		confidence_raw, trade_count, closed_sell_count, winning_sell_count,
		active_days, unique_non_quote_tokens, risky_token_count,
		unmatched_sell_count, missing_price_position_count,
		partial_valuation_count, transfer_in_count, transfer_out_count,
		history_incomplete, source_updated_at, source_updated_at_ms,
		calculated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
	          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		score.ChainID,
		score.WalletAddress,
		score.AnalyticsVersion,
		score.RealizedProfitUSDRaw,
		score.UnrealizedProfitUSDRaw,
		score.TotalProfitUSDRaw,
		score.TotalInvestedUSDRaw,
		score.ROIRaw,
		score.WinRateRaw,
		score.SmartScoreRaw,
		score.SmartScoreGrade,
		score.PerformanceScoreRaw,
		score.WinRateScoreRaw,
		score.TrackRecordScoreRaw,
		score.ActivityScoreRaw,
		score.RiskScoreRaw,
		score.ConfidenceRaw,
		score.TradeCount,
		score.ClosedSellCount,
		score.WinningSellCount,
		score.ActiveDays,
		score.UniqueNonQuoteTokens,
		score.RiskyTokenCount,
		score.UnmatchedSellCount,
		score.MissingPricePositionCount,
		score.PartialValuationCount,
		score.TransferInCount,
		score.TransferOutCount,
		boolToUInt8(score.HistoryIncomplete),
		safeDateTime(score.SourceUpdatedAt),
		score.SourceUpdatedAtMS,
		score.CalculatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert wallet smart score: %w", err)
	}
	return nil
}
