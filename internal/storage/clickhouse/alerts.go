package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/basewatch/base-analytics/internal/alerting"
)

func (s *EventStore) LargeTradeCandidates(
	ctx context.Context,
	lookback time.Duration,
	after alerting.Cursor,
	limit int,
) ([]alerting.Candidate, error) {
	if after.ValuedAt.IsZero() {
		after.ValuedAt = time.Unix(0, 0).UTC()
	}
	rows, err := s.conn.Query(ctx, `
		WITH latest_risk AS (
			SELECT
				chain_id,
				token_address,
				argMax(ifNull(is_honeypot, 0), fetched_at) AS latest_is_honeypot,
				argMax(ifNull(has_mint_method, 0), fetched_at) AS latest_has_mint_method,
				argMax(ifNull(has_black_method, 0), fetched_at) AS latest_has_black_method,
				argMax(ifNull(is_proxy, 0), fetched_at) AS latest_is_proxy
			FROM token_risk_snapshots
			GROUP BY chain_id, token_address
		)
		SELECT
			v.event_id,
			v.valuation_version,
			v.chain_id,
			v.block_number,
			v.block_time,
			v.transaction_hash,
			v.pool_address,
			v.protocol,
			v.protocol_version,
			v.bought_token_address,
			v.bought_token_symbol,
			v.sold_token_address,
			v.sold_token_symbol,
			v.trade_value_usd_raw,
			v.valued_at,
			b.latest_is_honeypot,
			b.latest_has_mint_method,
			b.latest_has_black_method,
			b.latest_is_proxy,
			s.latest_is_honeypot,
			s.latest_has_mint_method,
			s.latest_has_black_method,
			s.latest_is_proxy
		FROM dex_swap_valuations_current AS v
		LEFT JOIN latest_risk AS b
			ON b.chain_id = v.chain_id AND b.token_address = v.bought_token_address
		LEFT JOIN latest_risk AS s
			ON s.chain_id = v.chain_id AND s.token_address = v.sold_token_address
		WHERE v.is_large_trade = 1
		  AND v.valued_at >= now() - toIntervalSecond(?)
		  AND (v.valued_at, v.event_id) > (?, ?)
		ORDER BY v.valued_at, v.event_id
		LIMIT ?`,
		int64(lookback/time.Second),
		after.ValuedAt,
		after.EventID,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query large trade alert candidates: %w", err)
	}
	defer rows.Close()

	candidates := make([]alerting.Candidate, 0, limit)
	for rows.Next() {
		var candidate alerting.Candidate
		var boughtHoneypot, boughtMint, boughtBlack, boughtProxy uint8
		var soldHoneypot, soldMint, soldBlack, soldProxy uint8
		if err := rows.Scan(
			&candidate.EventID,
			&candidate.ValuationVersion,
			&candidate.ChainID,
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
			&boughtHoneypot,
			&boughtMint,
			&boughtBlack,
			&boughtProxy,
			&soldHoneypot,
			&soldMint,
			&soldBlack,
			&soldProxy,
		); err != nil {
			return nil, fmt.Errorf("scan large trade alert candidate: %w", err)
		}
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
		return nil, fmt.Errorf("iterate large trade alert candidates: %w", err)
	}
	return candidates, nil
}
