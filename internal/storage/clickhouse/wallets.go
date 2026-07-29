package clickhouse

import (
	"context"
	"fmt"

	"github.com/basewatch/base-analytics/internal/walletprofile"
)

func (s *EventStore) WalletActivityCandidates(
	ctx context.Context,
	limit int,
) ([]walletprofile.Candidate, error) {
	rows, err := s.conn.Query(ctx, `
		WITH
			latest_activities AS (
				SELECT
					event_id,
					argMax(source_valued_at, generated_at) AS latest_source_valued_at
				FROM wallet_swap_activities FINAL
				GROUP BY event_id
			),
			transactions AS (
				SELECT
					chain_id,
					transaction_hash,
					argMax(from_address, observed_at) AS wallet_address,
					argMax(to_address, observed_at) AS router_address
				FROM raw_transactions FINAL
				WHERE is_canonical = 1
				GROUP BY chain_id, transaction_hash
			)
		SELECT
			v.event_id,
			v.valuation_version,
			v.chain_id,
			t.wallet_address,
			t.router_address,
			v.block_number,
			v.block_time,
			v.transaction_hash,
			v.transaction_index,
			v.log_index,
			v.pool_address,
			v.protocol,
			v.protocol_version,
			v.bought_token_address,
			v.bought_token_symbol,
			if(
				v.bought_token_address = v.token0_address,
				v.token0_amount_raw,
				v.token1_amount_raw
			) AS bought_token_amount_raw,
			v.sold_token_address,
			v.sold_token_symbol,
			if(
				v.sold_token_address = v.token0_address,
				v.token0_amount_raw,
				v.token1_amount_raw
			) AS sold_token_amount_raw,
			v.trade_value_usd_raw,
			v.valuation_status,
			v.valued_at
		FROM dex_swap_valuations_current AS v
		INNER JOIN transactions AS t
			ON t.chain_id = v.chain_id
			AND t.transaction_hash = v.transaction_hash
		LEFT JOIN latest_activities AS a ON a.event_id = v.event_id
		WHERE t.wallet_address != ''
		  AND v.bought_token_address != ''
		  AND v.sold_token_address != ''
		  AND (
			a.event_id = ''
			OR a.latest_source_valued_at < v.valued_at
		  )
		ORDER BY v.valued_at, v.event_id
		LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query wallet activity candidates: %w", err)
	}
	defer rows.Close()

	candidates := make([]walletprofile.Candidate, 0, limit)
	for rows.Next() {
		var candidate walletprofile.Candidate
		if err := rows.Scan(
			&candidate.EventID,
			&candidate.ValuationVersion,
			&candidate.ChainID,
			&candidate.WalletAddress,
			&candidate.RouterAddress,
			&candidate.BlockNumber,
			&candidate.BlockTime,
			&candidate.TransactionHash,
			&candidate.TransactionIndex,
			&candidate.LogIndex,
			&candidate.PoolAddress,
			&candidate.Protocol,
			&candidate.ProtocolVersion,
			&candidate.BoughtTokenAddress,
			&candidate.BoughtTokenSymbol,
			&candidate.BoughtTokenAmountRaw,
			&candidate.SoldTokenAddress,
			&candidate.SoldTokenSymbol,
			&candidate.SoldTokenAmountRaw,
			&candidate.TradeValueUSDRaw,
			&candidate.ValuationStatus,
			&candidate.SourceValuedAt,
		); err != nil {
			return nil, fmt.Errorf("scan wallet activity candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wallet activity candidates: %w", err)
	}
	return candidates, nil
}

func (s *EventStore) InsertWalletActivities(
	ctx context.Context,
	activities []walletprofile.Activity,
) error {
	if len(activities) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO wallet_swap_activities (
		event_id, valuation_version, chain_id, wallet_address, router_address,
		attribution_method, block_number, block_time, transaction_hash,
		transaction_index, log_index, pool_address, protocol, protocol_version,
		bought_token_address, bought_token_symbol, bought_token_amount_raw,
		sold_token_address, sold_token_symbol, sold_token_amount_raw,
		trade_value_usd_raw, valuation_status, source_valued_at, generated_at
	)`)
	if err != nil {
		return fmt.Errorf("prepare wallet activity batch: %w", err)
	}
	for _, activity := range activities {
		if err := batch.Append(
			activity.EventID,
			activity.ValuationVersion,
			activity.ChainID,
			activity.WalletAddress,
			activity.RouterAddress,
			activity.AttributionMethod,
			activity.BlockNumber,
			activity.BlockTime,
			activity.TransactionHash,
			activity.TransactionIndex,
			activity.LogIndex,
			activity.PoolAddress,
			activity.Protocol,
			activity.ProtocolVersion,
			activity.BoughtTokenAddress,
			activity.BoughtTokenSymbol,
			activity.BoughtTokenAmountRaw,
			activity.SoldTokenAddress,
			activity.SoldTokenSymbol,
			activity.SoldTokenAmountRaw,
			activity.TradeValueUSDRaw,
			activity.ValuationStatus,
			activity.SourceValuedAt,
			activity.GeneratedAt,
		); err != nil {
			return fmt.Errorf("append wallet activity %s: %w", activity.EventID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send wallet activity batch: %w", err)
	}
	return nil
}
