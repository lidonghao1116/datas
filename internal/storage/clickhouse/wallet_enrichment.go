package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/basewatch/base-analytics/internal/walletenrichment"
)

func (s *EventStore) WalletEnrichmentCandidates(
	ctx context.Context,
	source string,
	period string,
	freshness time.Duration,
	activeLookback time.Duration,
	limit int,
) ([]walletenrichment.Candidate, error) {
	rows, err := s.conn.Query(ctx, `
		WITH
			active_wallets AS (
				SELECT
					chain_id,
					wallet_address,
					max(block_time) AS last_active_at
				FROM wallet_swap_activities FINAL
				WHERE block_time >= now() - toIntervalSecond(?)
				GROUP BY chain_id, wallet_address
			),
			latest_snapshots AS (
				SELECT
					chain_id,
					wallet_address,
					period,
					argMax(fetched_at, fetched_at) AS latest_fetched_at,
					argMax(expires_at, fetched_at) AS latest_expires_at
				FROM gmgn_wallet_profile_snapshots
				WHERE source = ? AND period = ?
				GROUP BY chain_id, wallet_address, period
			),
			latest_states AS (
				SELECT
					chain_id,
					wallet_address,
					period,
					argMax(next_retry_at, attempted_at) AS latest_next_retry_at
				FROM wallet_enrichment_sync_state FINAL
				WHERE source = ? AND period = ?
				GROUP BY chain_id, wallet_address, period
			)
		SELECT w.chain_id, w.wallet_address
		FROM active_wallets AS w
		LEFT JOIN latest_snapshots AS s
			ON s.chain_id = w.chain_id
			AND s.wallet_address = w.wallet_address
			AND s.period = ?
		LEFT JOIN latest_states AS st
			ON st.chain_id = w.chain_id
			AND st.wallet_address = w.wallet_address
			AND st.period = ?
		WHERE (
			s.wallet_address = ''
			OR s.latest_expires_at <= now()
			OR s.latest_fetched_at <= now() - toIntervalSecond(?)
		)
		  AND (
			st.wallet_address = ''
			OR st.latest_next_retry_at <= now()
		  )
		ORDER BY
			if(s.wallet_address = '', toDateTime64(0, 3), s.latest_fetched_at),
			w.last_active_at DESC,
			w.wallet_address
		LIMIT ?`,
		int64(activeLookback/time.Second),
		source,
		period,
		source,
		period,
		period,
		period,
		int64(freshness/time.Second),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query wallet enrichment candidates: %w", err)
	}
	defer rows.Close()

	candidates := make([]walletenrichment.Candidate, 0, limit)
	for rows.Next() {
		var candidate walletenrichment.Candidate
		candidate.Period = period
		if err := rows.Scan(&candidate.ChainID, &candidate.WalletAddress); err != nil {
			return nil, fmt.Errorf("scan wallet enrichment candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wallet enrichment candidates: %w", err)
	}
	return candidates, nil
}

func (s *EventStore) InsertWalletEnrichmentSnapshot(
	ctx context.Context,
	snapshot walletenrichment.Snapshot,
) error {
	stats := snapshot.Stats
	identity := stats.Identity
	err := s.conn.Exec(ctx, `
		INSERT INTO gmgn_wallet_profile_snapshots (
			chain_id, wallet_address, period, source, native_balance_raw,
			realized_profit_raw, unrealized_profit_raw, pnl_raw, win_rate_raw,
			total_cost_raw, buy_count, sell_count, token_count,
			avg_holding_seconds, display_name, ens, primary_tag, tags,
			twitter_username, twitter_name, twitter_followers,
			is_blue_verified, created_token_count, wallet_created_at,
			fund_from, fund_from_address, fund_amount_raw, source_updated_at,
			fetched_at, expires_at, raw_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshot.ChainID,
		stats.WalletAddress,
		stats.Period,
		snapshot.Source,
		stats.NativeBalanceRaw,
		stats.RealizedProfitRaw,
		stats.UnrealizedProfitRaw,
		stats.PnLRaw,
		stats.WinRateRaw,
		stats.TotalCostRaw,
		stats.BuyCount,
		stats.SellCount,
		stats.TokenCount,
		stats.AvgHoldingSeconds,
		identity.DisplayName,
		identity.ENS,
		identity.PrimaryTag,
		identity.Tags,
		identity.TwitterUsername,
		identity.TwitterName,
		identity.TwitterFollowers,
		boolToUInt8(identity.IsBlueVerified),
		identity.CreatedTokenCount,
		safeDateTime(identity.WalletCreatedAt),
		identity.FundFrom,
		identity.FundFromAddress,
		identity.FundAmountRaw,
		safeDateTime(stats.SourceUpdatedAt),
		snapshot.FetchedAt,
		snapshot.ExpiresAt,
		string(stats.RawJSON),
	)
	if err != nil {
		return fmt.Errorf("insert wallet enrichment snapshot: %w", err)
	}
	return nil
}

func (s *EventStore) RecordWalletEnrichmentSync(
	ctx context.Context,
	state walletenrichment.SyncState,
) error {
	var previousAttempts uint32
	err := s.conn.QueryRow(ctx, `
		SELECT ifNull(argMax(attempt_count, attempted_at), 0)
		FROM wallet_enrichment_sync_state
		WHERE source = ?
		  AND chain_id = ?
		  AND wallet_address = ?
		  AND period = ?`,
		state.Source,
		state.ChainID,
		state.WalletAddress,
		state.Period,
	).Scan(&previousAttempts)
	if err != nil {
		return fmt.Errorf("query wallet enrichment attempts: %w", err)
	}
	attempts := uint32(0)
	if state.Status == "failed" {
		attempts = previousAttempts + 1
	}
	err = s.conn.Exec(ctx, `
		INSERT INTO wallet_enrichment_sync_state (
			source, chain_id, wallet_address, period, status, last_error,
			attempt_count, attempted_at, next_retry_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		state.Source,
		state.ChainID,
		state.WalletAddress,
		state.Period,
		state.Status,
		state.LastError,
		attempts,
		state.AttemptedAt,
		state.NextRetryAt,
	)
	if err != nil {
		return fmt.Errorf("insert wallet enrichment sync state: %w", err)
	}
	return nil
}
