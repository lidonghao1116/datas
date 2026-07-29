package clickhouse

import (
	"context"
	"fmt"

	"github.com/basewatch/base-analytics/internal/gateway"
)

const walletAttributionMethod = "transaction_from"

func (s *EventStore) WalletProfile(
	ctx context.Context,
	chainID uint64,
	address string,
) (gateway.WalletProfile, error) {
	profile := gateway.WalletProfile{
		ChainID:           chainID,
		WalletAddress:     address,
		AttributionMethod: walletAttributionMethod,
	}
	var activityCount uint64
	row := s.conn.QueryRow(ctx, `
		SELECT
			count(),
			min(activity_time),
			max(activity_time),
			uniqExact(toDate(activity_time)),
			uniqExact(transaction_hash),
			max(recorded_at)
		FROM (
			SELECT block_time AS activity_time, transaction_hash, generated_at AS recorded_at
			FROM wallet_swap_activities FINAL
			WHERE chain_id = ? AND wallet_address = ?
			UNION ALL
			SELECT block_time AS activity_time, transaction_hash, observed_at AS recorded_at
			FROM erc20_transfers FINAL
			WHERE chain_id = ?
			  AND is_canonical = 1
			  AND (from_address = ? OR to_address = ?)
		)`,
		chainID,
		address,
		chainID,
		address,
		address,
	)
	if err := row.Scan(
		&activityCount,
		&profile.FirstActiveAt,
		&profile.LastActiveAt,
		&profile.ActiveDays,
		&profile.TransactionCount,
		&profile.ProfileUpdatedAt,
	); err != nil {
		return gateway.WalletProfile{}, fmt.Errorf("query wallet activity summary: %w", err)
	}
	if activityCount == 0 {
		return gateway.WalletProfile{}, gateway.ErrNotFound
	}

	row = s.conn.QueryRow(ctx, `
		SELECT
			uniqExact(transaction_hash),
			count(),
			countIf(
				upper(sold_token_symbol) IN ('WETH', 'USDC', 'USDBC', 'USDT', 'DAI', 'EURC', 'CBBTC')
				AND upper(bought_token_symbol) NOT IN ('WETH', 'USDC', 'USDBC', 'USDT', 'DAI', 'EURC', 'CBBTC')
			),
			countIf(
				upper(bought_token_symbol) IN ('WETH', 'USDC', 'USDBC', 'USDT', 'DAI', 'EURC', 'CBBTC')
				AND upper(sold_token_symbol) NOT IN ('WETH', 'USDC', 'USDBC', 'USDT', 'DAI', 'EURC', 'CBBTC')
			),
			countIf(
				(
					upper(bought_token_symbol) IN ('WETH', 'USDC', 'USDBC', 'USDT', 'DAI', 'EURC', 'CBBTC')
				) = (
					upper(sold_token_symbol) IN ('WETH', 'USDC', 'USDBC', 'USDT', 'DAI', 'EURC', 'CBBTC')
				)
			),
			toString(sum(toDecimal256OrZero(trade_value_usd_raw, 18)))
		FROM wallet_swap_activities FINAL
		WHERE chain_id = ? AND wallet_address = ?`,
		chainID,
		address,
	)
	if err := row.Scan(
		&profile.SwapTransactionCount,
		&profile.SwapCount,
		&profile.BuyCount,
		&profile.SellCount,
		&profile.OtherSwapCount,
		&profile.SwapVolumeUSDRaw,
	); err != nil {
		return gateway.WalletProfile{}, fmt.Errorf("query wallet swap summary: %w", err)
	}

	row = s.conn.QueryRow(ctx, `
		SELECT
			countIf(to_address = ?),
			countIf(from_address = ?)
		FROM erc20_transfers FINAL
		WHERE chain_id = ?
		  AND is_canonical = 1
		  AND (from_address = ? OR to_address = ?)`,
		address,
		address,
		chainID,
		address,
		address,
	)
	if err := row.Scan(&profile.TransferInCount, &profile.TransferOutCount); err != nil {
		return gateway.WalletProfile{}, fmt.Errorf("query wallet transfer summary: %w", err)
	}

	if profile.SwapCount > 0 {
		row = s.conn.QueryRow(ctx, `
			SELECT uniqExact(token_address)
			FROM (
				SELECT bought_token_address AS token_address
				FROM wallet_swap_activities FINAL
				WHERE chain_id = ? AND wallet_address = ?
				UNION ALL
				SELECT sold_token_address AS token_address
				FROM wallet_swap_activities FINAL
				WHERE chain_id = ? AND wallet_address = ?
			)`,
			chainID,
			address,
			chainID,
			address,
		)
		if err := row.Scan(&profile.UniqueSwapTokens); err != nil {
			return gateway.WalletProfile{}, fmt.Errorf("query unique wallet tokens: %w", err)
		}
		if err := s.populateWalletFavorites(ctx, &profile); err != nil {
			return gateway.WalletProfile{}, err
		}
	}
	return profile, nil
}

func (s *EventStore) populateWalletFavorites(
	ctx context.Context,
	profile *gateway.WalletProfile,
) error {
	row := s.conn.QueryRow(ctx, `
		SELECT token_address, argMax(token_symbol, block_time)
		FROM (
			SELECT bought_token_address AS token_address,
			       bought_token_symbol AS token_symbol,
			       block_time
			FROM wallet_swap_activities FINAL
			WHERE chain_id = ? AND wallet_address = ?
			UNION ALL
			SELECT sold_token_address AS token_address,
			       sold_token_symbol AS token_symbol,
			       block_time
			FROM wallet_swap_activities FINAL
			WHERE chain_id = ? AND wallet_address = ?
		)
		GROUP BY token_address
		ORDER BY count() DESC, token_address
		LIMIT 1`,
		profile.ChainID,
		profile.WalletAddress,
		profile.ChainID,
		profile.WalletAddress,
	)
	if err := row.Scan(&profile.FavoriteTokenAddress, &profile.FavoriteTokenSymbol); err != nil {
		return fmt.Errorf("query favorite wallet token: %w", err)
	}
	row = s.conn.QueryRow(ctx, `
		SELECT protocol
		FROM wallet_swap_activities FINAL
		WHERE chain_id = ? AND wallet_address = ?
		GROUP BY protocol
		ORDER BY count() DESC, protocol
		LIMIT 1`,
		profile.ChainID,
		profile.WalletAddress,
	)
	if err := row.Scan(&profile.FavoriteProtocol); err != nil {
		return fmt.Errorf("query favorite wallet protocol: %w", err)
	}
	return nil
}

func (s *EventStore) WalletTrades(
	ctx context.Context,
	chainID uint64,
	address string,
	limit int,
) ([]gateway.WalletTrade, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT
			event_id, chain_id, wallet_address, router_address,
			attribution_method, block_number, block_time, transaction_hash,
			pool_address, protocol, protocol_version, bought_token_address,
			bought_token_symbol, bought_token_amount_raw, sold_token_address,
			sold_token_symbol, sold_token_amount_raw, trade_value_usd_raw,
			valuation_status, source_valued_at
		FROM wallet_swap_activities FINAL
		WHERE chain_id = ? AND wallet_address = ?
		ORDER BY block_time DESC, transaction_hash DESC, log_index DESC
		LIMIT ?`,
		chainID,
		address,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query wallet trades: %w", err)
	}
	defer rows.Close()

	trades := make([]gateway.WalletTrade, 0, limit)
	for rows.Next() {
		var trade gateway.WalletTrade
		if err := rows.Scan(
			&trade.EventID,
			&trade.ChainID,
			&trade.WalletAddress,
			&trade.RouterAddress,
			&trade.AttributionMethod,
			&trade.BlockNumber,
			&trade.BlockTime,
			&trade.TransactionHash,
			&trade.PoolAddress,
			&trade.Protocol,
			&trade.ProtocolVersion,
			&trade.BoughtTokenAddress,
			&trade.BoughtTokenSymbol,
			&trade.BoughtTokenAmountRaw,
			&trade.SoldTokenAddress,
			&trade.SoldTokenSymbol,
			&trade.SoldTokenAmountRaw,
			&trade.TradeValueUSDRaw,
			&trade.ValuationStatus,
			&trade.SourceValuedAt,
		); err != nil {
			return nil, fmt.Errorf("scan wallet trade: %w", err)
		}
		trades = append(trades, trade)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wallet trades: %w", err)
	}
	return trades, nil
}

func (s *EventStore) WalletPositions(
	ctx context.Context,
	chainID uint64,
	address string,
	limit int,
) ([]gateway.WalletPosition, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT
			chain_id,
			wallet_address,
			token_address,
			argMax(token_symbol, block_time),
			toString(sum(toDecimal256OrZero(bought_amount_raw, 18))),
			toString(sum(toDecimal256OrZero(sold_amount_raw, 18))),
			toString(
				sum(toDecimal256OrZero(bought_amount_raw, 18))
				- sum(toDecimal256OrZero(sold_amount_raw, 18))
			),
			sum(buy_count),
			sum(sell_count),
			toString(sum(toDecimal256OrZero(trade_value_usd_raw, 18))),
			min(block_time),
			max(block_time)
		FROM (
			SELECT
				chain_id, wallet_address, bought_token_address AS token_address,
				bought_token_symbol AS token_symbol,
				bought_token_amount_raw AS bought_amount_raw,
				'0' AS sold_amount_raw,
				toUInt64(1) AS buy_count,
				toUInt64(0) AS sell_count,
				trade_value_usd_raw,
				block_time
			FROM wallet_swap_activities FINAL
			WHERE chain_id = ? AND wallet_address = ?
			UNION ALL
			SELECT
				chain_id, wallet_address, sold_token_address AS token_address,
				sold_token_symbol AS token_symbol,
				'0' AS bought_amount_raw,
				sold_token_amount_raw AS sold_amount_raw,
				toUInt64(0) AS buy_count,
				toUInt64(1) AS sell_count,
				trade_value_usd_raw,
				block_time
			FROM wallet_swap_activities FINAL
			WHERE chain_id = ? AND wallet_address = ?
		)
		GROUP BY chain_id, wallet_address, token_address
		ORDER BY abs(
			sum(toDecimal256OrZero(bought_amount_raw, 18))
			- sum(toDecimal256OrZero(sold_amount_raw, 18))
		) DESC, token_address
		LIMIT ?`,
		chainID,
		address,
		chainID,
		address,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query wallet positions: %w", err)
	}
	defer rows.Close()

	positions := make([]gateway.WalletPosition, 0, limit)
	for rows.Next() {
		var position gateway.WalletPosition
		if err := rows.Scan(
			&position.ChainID,
			&position.WalletAddress,
			&position.TokenAddress,
			&position.TokenSymbol,
			&position.BoughtAmountRaw,
			&position.SoldAmountRaw,
			&position.NetAmountRaw,
			&position.BuyCount,
			&position.SellCount,
			&position.SwapVolumeUSDRaw,
			&position.FirstTradedAt,
			&position.LastTradedAt,
		); err != nil {
			return nil, fmt.Errorf("scan wallet position: %w", err)
		}
		position.PositionBasis = "observed_swap_flow"
		positions = append(positions, position)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wallet positions: %w", err)
	}
	return positions, nil
}
