package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/basewatch/base-analytics/internal/alerting"
	"github.com/basewatch/base-analytics/internal/domain"
	"github.com/basewatch/base-analytics/internal/flashblocks"
)

func (s *EventStore) EnrichPendingSwap(
	ctx context.Context,
	swap domain.PoolSwap,
	walletAddress string,
	scoreVersion string,
) (flashblocks.Enrichment, bool, error) {
	enrichment := flashblocks.Enrichment{}
	var (
		token0Honeypot, token0Mint, token0Black, token0Proxy uint8
		token1Honeypot, token1Mint, token1Black, token1Proxy uint8
	)
	err := s.conn.QueryRow(ctx, `
		WITH
			pool_metadata AS (
				SELECT
					chain_id,
					pool_address,
					argMax(factory_address, observed_at) AS factory_address,
					argMax(protocol, observed_at) AS protocol,
					argMax(protocol_version, observed_at) AS protocol_version,
					argMax(token0_address, observed_at) AS token0_address,
					argMax(token1_address, observed_at) AS token1_address,
					argMax(token0_symbol, observed_at) AS token0_symbol,
					argMax(token1_symbol, observed_at) AS token1_symbol,
					argMax(token0_decimals, observed_at) AS token0_decimals,
					argMax(token1_decimals, observed_at) AS token1_decimals,
					argMax(metadata_status, observed_at) AS latest_metadata_status
				FROM dex_pool_swaps
				WHERE chain_id = ? AND pool_address = ?
				GROUP BY chain_id, pool_address
				HAVING latest_metadata_status IN ('resolved', 'partial')
			),
			latest_prices AS (
				SELECT
					chain_id,
					token_address,
					argMax(price_usd_raw, fetched_at) AS latest_price_raw,
					argMax(source, fetched_at) AS latest_price_source,
					argMax(source_updated_at, fetched_at) AS latest_source_updated_at,
					max(fetched_at) AS latest_fetched_at
				FROM token_market_snapshots
				WHERE chain_id = ?
				  AND (chain_id, token_address) IN (
					SELECT
						chain_id,
						arrayJoin([token0_address, token1_address]) AS token_address
					FROM pool_metadata
				  )
				GROUP BY chain_id, token_address
			),
			latest_score AS (
				SELECT
					chain_id,
					wallet_address,
					argMax(analytics_version, calculated_at) AS latest_score_version,
					argMax(smart_score_raw, calculated_at) AS latest_score_raw,
					argMax(smart_score_grade, calculated_at) AS latest_score_grade,
					argMax(confidence_raw, calculated_at) AS latest_confidence_raw,
					argMax(source_updated_at, calculated_at) AS latest_source_updated_at,
					max(calculated_at) AS latest_calculated_at
				FROM wallet_smart_score_snapshots
				WHERE chain_id = ? AND wallet_address = ? AND analytics_version = ?
				GROUP BY chain_id, wallet_address
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
				WHERE chain_id = ?
				  AND (chain_id, token_address) IN (
					SELECT
						chain_id,
						arrayJoin([token0_address, token1_address]) AS token_address
					FROM pool_metadata
				  )
				GROUP BY chain_id, token_address
			)
		SELECT
			pm.factory_address, pm.protocol, pm.protocol_version,
			pm.token0_address, pm.token1_address, pm.token0_symbol, pm.token1_symbol,
			pm.token0_decimals, pm.token1_decimals, pm.latest_metadata_status,
			p0.latest_price_raw, p0.latest_price_source,
			p0.latest_source_updated_at, p0.latest_fetched_at,
			p1.latest_price_raw, p1.latest_price_source,
			p1.latest_source_updated_at, p1.latest_fetched_at,
			sc.latest_score_version, sc.latest_score_raw, sc.latest_score_grade,
			sc.latest_confidence_raw, sc.latest_source_updated_at, sc.latest_calculated_at,
			r0.latest_is_honeypot, r0.latest_has_mint_method,
			r0.latest_has_black_method, r0.latest_is_proxy,
			r1.latest_is_honeypot, r1.latest_has_mint_method,
			r1.latest_has_black_method, r1.latest_is_proxy
		FROM pool_metadata AS pm
		LEFT JOIN latest_prices AS p0
			ON p0.chain_id = pm.chain_id AND p0.token_address = pm.token0_address
		LEFT JOIN latest_prices AS p1
			ON p1.chain_id = pm.chain_id AND p1.token_address = pm.token1_address
		LEFT JOIN latest_score AS sc
			ON sc.chain_id = pm.chain_id AND sc.wallet_address = ?
		LEFT JOIN latest_risk AS r0
			ON r0.chain_id = pm.chain_id AND r0.token_address = pm.token0_address
		LEFT JOIN latest_risk AS r1
			ON r1.chain_id = pm.chain_id AND r1.token_address = pm.token1_address`,
		swap.ChainID,
		swap.PoolAddress,
		swap.ChainID,
		swap.ChainID,
		walletAddress,
		scoreVersion,
		swap.ChainID,
		walletAddress,
	).Scan(
		&swap.FactoryAddress,
		&swap.Protocol,
		&swap.ProtocolVersion,
		&swap.Token0Address,
		&swap.Token1Address,
		&swap.Token0Symbol,
		&swap.Token1Symbol,
		&swap.Token0Decimals,
		&swap.Token1Decimals,
		&swap.MetadataStatus,
		&enrichment.Valuation.Price0.Raw,
		&enrichment.Valuation.Price0.Source,
		&enrichment.Valuation.Price0.SourceUpdatedAt,
		&enrichment.Valuation.Price0.FetchedAt,
		&enrichment.Valuation.Price1.Raw,
		&enrichment.Valuation.Price1.Source,
		&enrichment.Valuation.Price1.SourceUpdatedAt,
		&enrichment.Valuation.Price1.FetchedAt,
		&enrichment.SmartVersion,
		&enrichment.SmartScore,
		&enrichment.SmartGrade,
		&enrichment.SmartConfidence,
		&enrichment.SmartSourceAt,
		&enrichment.SmartCalculated,
		&token0Honeypot,
		&token0Mint,
		&token0Black,
		&token0Proxy,
		&token1Honeypot,
		&token1Mint,
		&token1Black,
		&token1Proxy,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return flashblocks.Enrichment{}, false, nil
		}
		return flashblocks.Enrichment{}, false, fmt.Errorf("enrich pending swap: %w", err)
	}
	enrichment.Valuation.Swap = swap
	enrichment.Token0Risk = alerting.RiskFlags{
		IsHoneypot:     token0Honeypot == 1,
		HasMintMethod:  token0Mint == 1,
		HasBlackMethod: token0Black == 1,
		IsProxy:        token0Proxy == 1,
	}
	enrichment.Token1Risk = alerting.RiskFlags{
		IsHoneypot:     token1Honeypot == 1,
		HasMintMethod:  token1Mint == 1,
		HasBlackMethod: token1Black == 1,
		IsProxy:        token1Proxy == 1,
	}
	return enrichment, true, nil
}
