package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/basewatch/base-analytics/internal/gateway"
)

func (s *EventStore) RecentLargeTrades(
	ctx context.Context,
	limit int,
) ([]gateway.LargeTrade, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT
			event_id, block_number, block_time, transaction_hash, pool_address,
			protocol, protocol_version, bought_token_address, bought_token_symbol,
			sold_token_address, sold_token_symbol, trade_value_usd_raw,
			valuation_status, valued_at
		FROM dex_swap_valuations_current
		WHERE is_large_trade = 1
		ORDER BY valued_at DESC, event_id DESC
		LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query recent large trades: %w", err)
	}
	defer rows.Close()

	trades := make([]gateway.LargeTrade, 0, limit)
	for rows.Next() {
		var trade gateway.LargeTrade
		if err := rows.Scan(
			&trade.EventID,
			&trade.BlockNumber,
			&trade.BlockTime,
			&trade.TransactionHash,
			&trade.PoolAddress,
			&trade.Protocol,
			&trade.ProtocolVersion,
			&trade.BoughtTokenAddress,
			&trade.BoughtTokenSymbol,
			&trade.SoldTokenAddress,
			&trade.SoldTokenSymbol,
			&trade.TradeValueUSDRaw,
			&trade.ValuationStatus,
			&trade.ValuedAt,
		); err != nil {
			return nil, fmt.Errorf("scan recent large trade: %w", err)
		}
		trades = append(trades, trade)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recent large trades: %w", err)
	}
	return trades, nil
}

func (s *EventStore) TokenMarket(
	ctx context.Context,
	chainID uint64,
	address string,
) (gateway.TokenMarket, error) {
	rows, err := s.conn.Query(ctx, `
		WITH
			latest_market AS (
				SELECT
					chain_id,
					token_address,
					argMax(source, fetched_at) AS latest_source,
					argMax(price_usd_raw, fetched_at) AS latest_price_usd_raw,
					argMax(price_change_24h_raw, fetched_at) AS latest_price_change_24h_raw,
					argMax(tvl_usd_raw, fetched_at) AS latest_tvl_usd_raw,
					argMax(market_cap_usd_raw, fetched_at) AS latest_market_cap_usd_raw,
					argMax(fdv_usd_raw, fetched_at) AS latest_fdv_usd_raw,
					argMax(volume_24h_usd_raw, fetched_at) AS latest_volume_24h_usd_raw,
					argMax(holders, fetched_at) AS latest_holders,
					max(fetched_at) AS latest_market_fetched_at
				FROM token_market_snapshots
				WHERE chain_id = ? AND token_address = ?
				GROUP BY chain_id, token_address
			),
			latest_risk AS (
				SELECT
					chain_id,
					token_address,
					argMax(risk_score_raw, fetched_at) AS latest_risk_score_raw,
					argMax(is_honeypot, fetched_at) AS latest_is_honeypot,
					argMax(has_mint_method, fetched_at) AS latest_has_mint_method,
					argMax(has_black_method, fetched_at) AS latest_has_black_method,
					argMax(is_proxy, fetched_at) AS latest_is_proxy,
					max(fetched_at) AS latest_risk_fetched_at
				FROM token_risk_snapshots
				WHERE chain_id = ? AND token_address = ?
				GROUP BY chain_id, token_address
			)
		SELECT
			m.chain_id, m.token_address, m.latest_source,
			m.latest_price_usd_raw, m.latest_price_change_24h_raw,
			m.latest_tvl_usd_raw, m.latest_market_cap_usd_raw,
			m.latest_fdv_usd_raw, m.latest_volume_24h_usd_raw,
			m.latest_holders, m.latest_market_fetched_at,
			r.latest_risk_score_raw, r.latest_is_honeypot,
			r.latest_has_mint_method, r.latest_has_black_method,
			r.latest_is_proxy, r.latest_risk_fetched_at
		FROM latest_market AS m
		LEFT JOIN latest_risk AS r
			ON r.chain_id = m.chain_id AND r.token_address = m.token_address`,
		chainID,
		address,
		chainID,
		address,
	)
	if err != nil {
		return gateway.TokenMarket{}, fmt.Errorf("query token market: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return gateway.TokenMarket{}, fmt.Errorf("iterate token market: %w", err)
		}
		return gateway.TokenMarket{}, gateway.ErrNotFound
	}
	var market gateway.TokenMarket
	var riskFetchedAt time.Time
	if err := rows.Scan(
		&market.ChainID,
		&market.TokenAddress,
		&market.Source,
		&market.PriceUSDRaw,
		&market.PriceChange24hRaw,
		&market.TVLUSDRaw,
		&market.MarketCapUSDRaw,
		&market.FDVUSDRaw,
		&market.Volume24hUSDRaw,
		&market.Holders,
		&market.MarketUpdatedAt,
		&market.RiskScoreRaw,
		&market.IsHoneypot,
		&market.HasMintMethod,
		&market.HasBlackMethod,
		&market.IsProxy,
		&riskFetchedAt,
	); err != nil {
		return gateway.TokenMarket{}, fmt.Errorf("scan token market: %w", err)
	}
	if !riskFetchedAt.Equal(time.Unix(0, 0).UTC()) && !riskFetchedAt.IsZero() {
		market.RiskUpdatedAt = &riskFetchedAt
	}
	return market, nil
}

func (s *EventStore) Ping(ctx context.Context) error {
	return s.conn.Ping(ctx)
}
