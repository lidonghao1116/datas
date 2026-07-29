package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/basewatch/base-analytics/internal/gateway"
	"github.com/basewatch/base-analytics/internal/walletanalytics"
)

func (s *EventStore) WalletSmartScore(
	ctx context.Context,
	chainID uint64,
	address string,
) (gateway.WalletSmartScore, error) {
	row := s.conn.QueryRow(ctx, `
		SELECT
			chain_id, wallet_address, analytics_version,
			realized_profit_usd_raw, unrealized_profit_usd_raw,
			total_profit_usd_raw, total_invested_usd_raw, roi_raw,
			win_rate_raw, smart_score_raw, smart_score_grade,
			performance_score_raw, win_rate_score_raw, track_record_score_raw,
			activity_score_raw, risk_score_raw, confidence_raw, trade_count,
			closed_sell_count, winning_sell_count, active_days,
			unique_non_quote_tokens, risky_token_count, unmatched_sell_count,
			missing_price_position_count, partial_valuation_count,
			transfer_in_count, transfer_out_count, history_incomplete,
			source_updated_at, calculated_at
		FROM wallet_smart_score_snapshots
		WHERE chain_id = ?
		  AND wallet_address = ?
		  AND analytics_version = ?
		ORDER BY calculated_at DESC
		LIMIT 1`,
		chainID,
		address,
		walletanalytics.Version,
	)
	var score gateway.WalletSmartScore
	var historyIncomplete uint8
	if err := row.Scan(
		&score.ChainID,
		&score.WalletAddress,
		&score.AnalyticsVersion,
		&score.RealizedProfitUSDRaw,
		&score.UnrealizedProfitUSDRaw,
		&score.TotalProfitUSDRaw,
		&score.TotalInvestedUSDRaw,
		&score.ROIRaw,
		&score.WinRateRaw,
		&score.SmartScoreRaw,
		&score.SmartScoreGrade,
		&score.PerformanceScoreRaw,
		&score.WinRateScoreRaw,
		&score.TrackRecordScoreRaw,
		&score.ActivityScoreRaw,
		&score.RiskScoreRaw,
		&score.ConfidenceRaw,
		&score.TradeCount,
		&score.ClosedSellCount,
		&score.WinningSellCount,
		&score.ActiveDays,
		&score.UniqueNonQuoteTokens,
		&score.RiskyTokenCount,
		&score.UnmatchedSellCount,
		&score.MissingPricePositionCount,
		&score.PartialValuationCount,
		&score.TransferInCount,
		&score.TransferOutCount,
		&historyIncomplete,
		&score.SourceUpdatedAt,
		&score.CalculatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return gateway.WalletSmartScore{}, gateway.ErrNotFound
		}
		return gateway.WalletSmartScore{}, fmt.Errorf("query wallet smart score: %w", err)
	}
	score.HistoryIncomplete = historyIncomplete == 1
	return score, nil
}

func (s *EventStore) WalletTokenPnL(
	ctx context.Context,
	chainID uint64,
	address string,
	limit int,
) ([]gateway.WalletTokenPnL, error) {
	rows, err := s.conn.Query(ctx, `
		WITH latest AS (
			SELECT token_address, max(calculated_at) AS latest_calculated_at
			FROM wallet_token_pnl_snapshots
			WHERE chain_id = ?
			  AND wallet_address = ?
			  AND analytics_version = ?
			GROUP BY token_address
		)
		SELECT
			p.chain_id, p.wallet_address, p.token_address, p.token_symbol,
			p.analytics_version, p.is_quote_token, p.bought_amount_raw,
			p.sold_amount_raw, p.remaining_amount_raw,
			p.total_buy_cost_usd_raw, p.total_sell_income_usd_raw,
			p.remaining_cost_usd_raw, p.realized_profit_usd_raw,
			p.unrealized_profit_usd_raw, p.total_profit_usd_raw,
			p.current_value_usd_raw, p.average_cost_usd_raw,
			p.current_price_usd_raw, p.buy_count, p.sell_count,
			p.winning_sell_count, p.unmatched_sell_amount_raw,
			p.unmatched_sell_usd_raw, p.is_honeypot, p.has_mint_method,
			p.has_black_method, p.is_proxy, p.data_quality,
			p.first_traded_at, p.last_traded_at, p.price_updated_at,
			p.source_updated_at, p.calculated_at
		FROM wallet_token_pnl_snapshots AS p
		INNER JOIN latest AS l
			ON l.token_address = p.token_address
			AND l.latest_calculated_at = p.calculated_at
		WHERE p.chain_id = ?
		  AND p.wallet_address = ?
		  AND p.analytics_version = ?
		ORDER BY
			p.is_quote_token,
			abs(toDecimal256OrZero(p.total_profit_usd_raw, 18)) DESC,
			p.token_address
		LIMIT ?`,
		chainID,
		address,
		walletanalytics.Version,
		chainID,
		address,
		walletanalytics.Version,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query wallet token PnL: %w", err)
	}
	defer rows.Close()
	results := make([]gateway.WalletTokenPnL, 0, limit)
	for rows.Next() {
		var item gateway.WalletTokenPnL
		var quote, honeypot, mint, black, proxy uint8
		var priceUpdatedAt time.Time
		if err := rows.Scan(
			&item.ChainID,
			&item.WalletAddress,
			&item.TokenAddress,
			&item.TokenSymbol,
			&item.AnalyticsVersion,
			&quote,
			&item.BoughtAmountRaw,
			&item.SoldAmountRaw,
			&item.RemainingAmountRaw,
			&item.TotalBuyCostUSDRaw,
			&item.TotalSellIncomeUSDRaw,
			&item.RemainingCostUSDRaw,
			&item.RealizedProfitUSDRaw,
			&item.UnrealizedProfitUSDRaw,
			&item.TotalProfitUSDRaw,
			&item.CurrentValueUSDRaw,
			&item.AverageCostUSDRaw,
			&item.CurrentPriceUSDRaw,
			&item.BuyCount,
			&item.SellCount,
			&item.WinningSellCount,
			&item.UnmatchedSellAmountRaw,
			&item.UnmatchedSellUSDRaw,
			&honeypot,
			&mint,
			&black,
			&proxy,
			&item.DataQuality,
			&item.FirstTradedAt,
			&item.LastTradedAt,
			&priceUpdatedAt,
			&item.SourceUpdatedAt,
			&item.CalculatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan wallet token PnL: %w", err)
		}
		item.IsQuoteToken = quote == 1
		item.IsHoneypot = honeypot == 1
		item.HasMintMethod = mint == 1
		item.HasBlackMethod = black == 1
		item.IsProxy = proxy == 1
		if !priceUpdatedAt.Equal(time.Unix(0, 0).UTC()) && !priceUpdatedAt.IsZero() {
			item.PriceUpdatedAt = &priceUpdatedAt
		}
		results = append(results, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wallet token PnL: %w", err)
	}
	return results, nil
}
