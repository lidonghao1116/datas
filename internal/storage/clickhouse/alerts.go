package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/basewatch/base-analytics/internal/alerting"
)

func (s *EventStore) RealtimeAlertCandidates(
	ctx context.Context,
	lookback time.Duration,
	after alerting.Cursor,
	scoreVersion string,
	limit int,
) ([]alerting.Candidate, error) {
	if after.ObservedAt.IsZero() {
		after.ObservedAt = time.Unix(0, 0).UTC()
	}
	rows, err := s.conn.Query(ctx, `
		WITH
			candidate_activities AS (
				SELECT *
				FROM wallet_swap_activities FINAL
				WHERE generated_at >= now() - toIntervalSecond(?)
				  AND (generated_at, event_id) > (?, ?)
				ORDER BY generated_at, event_id
				LIMIT ?
			),
			latest_risk AS (
				SELECT
					chain_id,
					token_address,
					argMax(ifNull(is_honeypot, 0), fetched_at) AS latest_is_honeypot,
					argMax(ifNull(has_mint_method, 0), fetched_at) AS latest_has_mint_method,
					argMax(ifNull(has_black_method, 0), fetched_at) AS latest_has_black_method,
					argMax(ifNull(is_proxy, 0), fetched_at) AS latest_is_proxy
				FROM token_risk_snapshots
				WHERE (chain_id, token_address) IN (
					SELECT
						chain_id,
						arrayJoin([bought_token_address, sold_token_address]) AS token_address
					FROM candidate_activities
				)
				GROUP BY chain_id, token_address
			),
			latest_scores AS (
				SELECT
					chain_id,
					wallet_address,
					argMax(analytics_version, calculated_at) AS score_version,
					argMax(smart_score_raw, calculated_at) AS score_raw,
					argMax(smart_score_grade, calculated_at) AS score_grade,
					argMax(confidence_raw, calculated_at) AS confidence_raw,
					argMax(source_updated_at, calculated_at) AS score_source_updated_at,
					max(calculated_at) AS score_calculated_at
				FROM wallet_smart_score_snapshots
				WHERE analytics_version = ?
				  AND (chain_id, wallet_address) IN (
					SELECT chain_id, wallet_address
					FROM candidate_activities
				  )
				GROUP BY chain_id, wallet_address
			)
		SELECT
			w.event_id,
			w.valuation_version,
			w.chain_id,
			w.wallet_address,
			w.attribution_method,
			w.block_number,
			w.block_time,
			w.transaction_hash,
			w.pool_address,
			w.protocol,
			w.protocol_version,
			w.bought_token_address,
			w.bought_token_symbol,
			w.sold_token_address,
			w.sold_token_symbol,
			w.trade_value_usd_raw,
			w.source_valued_at,
			w.generated_at,
			v.is_large_trade,
			ifNull(sc.score_version, ''),
			ifNull(sc.score_raw, ''),
			ifNull(sc.score_grade, ''),
			ifNull(sc.confidence_raw, ''),
			ifNull(sc.score_source_updated_at, toDateTime64(0, 3, 'UTC')),
			ifNull(sc.score_calculated_at, toDateTime64(0, 3, 'UTC')),
			b.latest_is_honeypot,
			b.latest_has_mint_method,
			b.latest_has_black_method,
			b.latest_is_proxy,
			s.latest_is_honeypot,
			s.latest_has_mint_method,
			s.latest_has_black_method,
			s.latest_is_proxy
		FROM candidate_activities AS w
		INNER JOIN dex_swap_valuations_current AS v
			ON v.chain_id = w.chain_id AND v.event_id = w.event_id
		LEFT JOIN latest_scores AS sc
			ON sc.chain_id = w.chain_id AND sc.wallet_address = w.wallet_address
		LEFT JOIN latest_risk AS b
			ON b.chain_id = w.chain_id AND b.token_address = w.bought_token_address
		LEFT JOIN latest_risk AS s
			ON s.chain_id = w.chain_id AND s.token_address = w.sold_token_address
		ORDER BY w.generated_at, w.event_id
		`,
		int64(lookback/time.Second),
		after.ObservedAt,
		after.EventID,
		limit,
		scoreVersion,
	)
	if err != nil {
		return nil, fmt.Errorf("query realtime alert candidates: %w", err)
	}
	defer rows.Close()

	candidates := make([]alerting.Candidate, 0, limit)
	for rows.Next() {
		var candidate alerting.Candidate
		var isLargeTrade uint8
		var boughtHoneypot, boughtMint, boughtBlack, boughtProxy uint8
		var soldHoneypot, soldMint, soldBlack, soldProxy uint8
		if err := rows.Scan(
			&candidate.EventID,
			&candidate.ValuationVersion,
			&candidate.ChainID,
			&candidate.WalletAddress,
			&candidate.AttributionMethod,
			&candidate.BlockNumber,
			&candidate.BlockTime,
			&candidate.TransactionHash,
			&candidate.PoolAddress,
			&candidate.Protocol,
			&candidate.ProtocolVersion,
			&candidate.BoughtTokenAddress,
			&candidate.BoughtTokenSymbol,
			&candidate.SoldTokenAddress,
			&candidate.SoldTokenSymbol,
			&candidate.TradeValueUSDRaw,
			&candidate.ValuedAt,
			&candidate.ObservedAt,
			&isLargeTrade,
			&candidate.SmartScoreVersion,
			&candidate.SmartScoreRaw,
			&candidate.SmartScoreGrade,
			&candidate.SmartConfidenceRaw,
			&candidate.SmartScoreSourceAt,
			&candidate.SmartScoreAt,
			&boughtHoneypot,
			&boughtMint,
			&boughtBlack,
			&boughtProxy,
			&soldHoneypot,
			&soldMint,
			&soldBlack,
			&soldProxy,
		); err != nil {
			return nil, fmt.Errorf("scan realtime alert candidate: %w", err)
		}
		candidate.IsLargeTrade = isLargeTrade == 1
		candidate.BoughtRisk = alerting.RiskFlags{
			IsHoneypot:     boughtHoneypot == 1,
			HasMintMethod:  boughtMint == 1,
			HasBlackMethod: boughtBlack == 1,
			IsProxy:        boughtProxy == 1,
		}
		candidate.SoldRisk = alerting.RiskFlags{
			IsHoneypot:     soldHoneypot == 1,
			HasMintMethod:  soldMint == 1,
			HasBlackMethod: soldBlack == 1,
			IsProxy:        soldProxy == 1,
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate realtime alert candidates: %w", err)
	}
	return candidates, nil
}
