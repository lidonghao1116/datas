package clickhouse

import (
	"context"
	"fmt"

	"github.com/basewatch/base-analytics/internal/marketdata"
)

func (s *EventStore) InsertMarketSnapshots(
	ctx context.Context,
	snapshots []marketdata.MarketSnapshot,
) error {
	if len(snapshots) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO token_market_snapshots (
		chain_id, token_address, source, price_usd_raw, price_change_24h_raw,
		tvl_usd_raw, market_cap_usd_raw, fdv_usd_raw, volume_24h_usd_raw,
		holders, source_updated_at, fetched_at
	)`)
	if err != nil {
		return fmt.Errorf("prepare token market snapshot batch: %w", err)
	}
	for _, snapshot := range snapshots {
		if err := batch.Append(
			snapshot.ChainID,
			snapshot.Address,
			snapshot.Source,
			snapshot.PriceUSDRaw,
			snapshot.PriceChange24hRaw,
			snapshot.TVLUSDRaw,
			snapshot.MarketCapUSDRaw,
			snapshot.FDVUSDRaw,
			snapshot.Volume24hUSDRaw,
			snapshot.Holders,
			snapshot.SourceUpdatedAt,
			snapshot.FetchedAt,
		); err != nil {
			return fmt.Errorf("append token market snapshot %s: %w", snapshot.Address, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send token market snapshot batch: %w", err)
	}
	return nil
}

func (s *EventStore) InsertRiskSnapshots(
	ctx context.Context,
	snapshots []marketdata.RiskSnapshot,
) error {
	if len(snapshots) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO token_risk_snapshots (
		chain_id, token_address, source, risk_score_raw, is_honeypot,
		has_mint_method, has_black_method, is_proxy, owner_address,
		buy_tax_raw, sell_tax_raw, raw_json, source_updated_at, fetched_at
	)`)
	if err != nil {
		return fmt.Errorf("prepare token risk snapshot batch: %w", err)
	}
	for _, snapshot := range snapshots {
		if err := batch.Append(
			snapshot.ChainID,
			snapshot.Address,
			snapshot.Source,
			snapshot.RiskScoreRaw,
			snapshot.IsHoneypot,
			snapshot.HasMintMethod,
			snapshot.HasBlackMethod,
			snapshot.IsProxy,
			snapshot.OwnerAddress,
			snapshot.BuyTaxRaw,
			snapshot.SellTaxRaw,
			string(snapshot.RawJSON),
			snapshot.SourceUpdatedAt,
			snapshot.FetchedAt,
		); err != nil {
			return fmt.Errorf("append token risk snapshot %s: %w", snapshot.Address, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send token risk snapshot batch: %w", err)
	}
	return nil
}
