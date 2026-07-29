package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/basewatch/base-analytics/internal/valuation"
)

func (s *EventStore) ValuationCandidates(
	ctx context.Context,
	lookback time.Duration,
	maxPriceAge time.Duration,
	limit int,
) ([]valuation.Candidate, error) {
	rows, err := s.conn.Query(ctx, `
		WITH
			latest_prices AS (
				SELECT
					chain_id,
					token_address,
					argMax(price_usd_raw, fetched_at) AS latest_price_usd_raw,
					argMax(source, fetched_at) AS latest_source,
					argMax(source_updated_at, fetched_at) AS latest_source_updated_at,
					max(fetched_at) AS latest_fetched_at
				FROM token_market_snapshots
				GROUP BY chain_id, token_address
			),
			latest_valuations AS (
				SELECT
					event_id,
					argMax(swap_observed_at, valued_at) AS latest_swap_observed_at,
					argMax(valuation_version, valued_at) AS latest_valuation_version
				FROM dex_swap_valuations FINAL
				GROUP BY event_id
			)
		SELECT
			s.schema_version, s.chain_id, s.block_number, s.block_hash, s.block_time,
			s.transaction_hash, s.transaction_index, s.log_index, s.pool_address,
			s.factory_address, s.protocol, s.protocol_version, s.protocol_family,
			s.token0_address, s.token1_address, s.token0_symbol, s.token1_symbol,
			s.token0_decimals, s.token1_decimals, s.metadata_status, s.sender_address,
			s.recipient_address, s.amount0_delta_raw, s.amount1_delta_raw,
			s.sqrt_price_x96_raw, s.liquidity_raw, s.tick, s.parser_version,
			s.observed_at, s.is_canonical,
			p0.latest_price_usd_raw, p0.latest_source,
			p0.latest_source_updated_at, p0.latest_fetched_at,
			p1.latest_price_usd_raw, p1.latest_source,
			p1.latest_source_updated_at, p1.latest_fetched_at
		FROM dex_pool_swaps AS s FINAL
		LEFT JOIN latest_valuations AS v ON v.event_id = s.event_id
		LEFT JOIN latest_prices AS p0
			ON p0.chain_id = s.chain_id AND p0.token_address = s.token0_address
		LEFT JOIN latest_prices AS p1
			ON p1.chain_id = s.chain_id AND p1.token_address = s.token1_address
		WHERE s.is_canonical = 1
		  AND s.metadata_status IN ('resolved', 'partial')
		  AND s.observed_at >= now() - toIntervalSecond(?)
		  AND (
			(p0.latest_price_usd_raw != '' AND abs(dateDiff('second', s.observed_at, p0.latest_fetched_at)) <= ?)
			OR
			(p1.latest_price_usd_raw != '' AND abs(dateDiff('second', s.observed_at, p1.latest_fetched_at)) <= ?)
		  )
		  AND (
			v.event_id = ''
			OR v.latest_swap_observed_at < s.observed_at
			OR v.latest_valuation_version != ?
		  )
		ORDER BY s.observed_at, s.event_id
		LIMIT ?`,
		int64(lookback/time.Second),
		int64(maxPriceAge/time.Second),
		int64(maxPriceAge/time.Second),
		valuation.Version,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query swap valuation candidates: %w", err)
	}
	defer rows.Close()

	candidates := make([]valuation.Candidate, 0, limit)
	for rows.Next() {
		var candidate valuation.Candidate
		swap := &candidate.Swap
		if err := rows.Scan(
			&swap.SchemaVersion,
			&swap.ChainID,
			&swap.BlockNumber,
			&swap.BlockHash,
			&swap.BlockTime,
			&swap.TransactionHash,
			&swap.TransactionIndex,
			&swap.LogIndex,
			&swap.PoolAddress,
			&swap.FactoryAddress,
			&swap.Protocol,
			&swap.ProtocolVersion,
			&swap.ProtocolFamily,
			&swap.Token0Address,
			&swap.Token1Address,
			&swap.Token0Symbol,
			&swap.Token1Symbol,
			&swap.Token0Decimals,
			&swap.Token1Decimals,
			&swap.MetadataStatus,
			&swap.SenderAddress,
			&swap.RecipientAddress,
			&swap.Amount0DeltaRaw,
			&swap.Amount1DeltaRaw,
			&swap.SqrtPriceX96Raw,
			&swap.LiquidityRaw,
			&swap.Tick,
			&swap.ParserVersion,
			&swap.ObservedAt,
			&swap.IsCanonical,
			&candidate.Price0.Raw,
			&candidate.Price0.Source,
			&candidate.Price0.SourceUpdatedAt,
			&candidate.Price0.FetchedAt,
			&candidate.Price1.Raw,
			&candidate.Price1.Source,
			&candidate.Price1.SourceUpdatedAt,
			&candidate.Price1.FetchedAt,
		); err != nil {
			return nil, fmt.Errorf("scan swap valuation candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate swap valuation candidates: %w", err)
	}
	return candidates, nil
}

func (s *EventStore) InsertValuations(
	ctx context.Context,
	results []valuation.Result,
) error {
	if len(results) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO dex_swap_valuations (
		event_id, chain_id, block_number, block_time, transaction_hash,
		transaction_index, log_index, pool_address, protocol, protocol_version,
		token0_address, token1_address, token0_symbol, token1_symbol,
		token0_amount_raw, token1_amount_raw, token0_price_usd_raw,
		token1_price_usd_raw, token0_value_usd_raw, token1_value_usd_raw,
		trade_value_usd_raw, bought_token_address, bought_token_symbol,
		sold_token_address, sold_token_symbol, valuation_status, valuation_version,
		is_large_trade, price_source, token0_price_updated_at, token1_price_updated_at,
		swap_observed_at, valued_at
	)`)
	if err != nil {
		return fmt.Errorf("prepare swap valuation batch: %w", err)
	}
	for _, result := range results {
		swap := result.Swap
		if err := batch.Append(
			result.EventID,
			swap.ChainID,
			swap.BlockNumber,
			swap.BlockTime,
			swap.TransactionHash,
			swap.TransactionIndex,
			swap.LogIndex,
			swap.PoolAddress,
			swap.Protocol,
			swap.ProtocolVersion,
			swap.Token0Address,
			swap.Token1Address,
			swap.Token0Symbol,
			swap.Token1Symbol,
			result.Token0AmountRaw,
			result.Token1AmountRaw,
			result.Token0PriceUSDRaw,
			result.Token1PriceUSDRaw,
			result.Token0ValueUSDRaw,
			result.Token1ValueUSDRaw,
			result.TradeValueUSDRaw,
			result.BoughtTokenAddress,
			result.BoughtTokenSymbol,
			result.SoldTokenAddress,
			result.SoldTokenSymbol,
			result.Status,
			result.Version,
			result.IsLargeTrade,
			result.PriceSource,
			safeDateTime(result.Token0PriceUpdatedAt),
			safeDateTime(result.Token1PriceUpdatedAt),
			swap.ObservedAt,
			result.ValuedAt,
		); err != nil {
			return fmt.Errorf("append swap valuation %s: %w", result.EventID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send swap valuation batch: %w", err)
	}
	return nil
}

func safeDateTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Unix(0, 0).UTC()
	}
	return value
}
