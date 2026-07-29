package clickhouse

import (
	"context"
	"fmt"
	"time"

	clickhouseclient "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/basewatch/base-analytics/internal/parser/logs"
)

type EventStore struct {
	conn driver.Conn
}

func OpenEventStore(
	ctx context.Context,
	addr, database, username, password string,
) (*EventStore, error) {
	conn, err := clickhouseclient.Open(&clickhouseclient.Options{
		Addr: []string{addr},
		Auth: clickhouseclient.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		DialTimeout: 10 * time.Second,
		Compression: &clickhouseclient.Compression{
			Method: clickhouseclient.CompressionZSTD,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping ClickHouse: %w", err)
	}
	return &EventStore{conn: conn}, nil
}

func (s *EventStore) Insert(ctx context.Context, result logs.Result) error {
	if err := s.insertTransfers(ctx, result); err != nil {
		return err
	}
	if err := s.insertSwaps(ctx, result); err != nil {
		return err
	}
	return nil
}

func (s *EventStore) insertTransfers(ctx context.Context, result logs.Result) error {
	if len(result.Transfers) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO erc20_transfers (
		event_id, schema_version, chain_id, block_number, block_hash, block_time,
		transaction_hash, transaction_index, log_index, token_address,
		from_address, to_address, amount_raw, parser_version, observed_at,
		is_canonical
	)`)
	if err != nil {
		return fmt.Errorf("prepare ERC-20 transfer batch: %w", err)
	}
	for _, transfer := range result.Transfers {
		if err := batch.Append(
			transfer.EventID(),
			transfer.SchemaVersion,
			transfer.ChainID,
			transfer.BlockNumber,
			transfer.BlockHash,
			transfer.BlockTime,
			transfer.TransactionHash,
			transfer.TransactionIndex,
			transfer.LogIndex,
			transfer.TokenAddress,
			transfer.FromAddress,
			transfer.ToAddress,
			transfer.AmountRaw,
			transfer.ParserVersion,
			transfer.ObservedAt,
			transfer.IsCanonical,
		); err != nil {
			return fmt.Errorf("append ERC-20 transfer %s: %w", transfer.EventID(), err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send ERC-20 transfer batch: %w", err)
	}
	return nil
}

func (s *EventStore) insertSwaps(ctx context.Context, result logs.Result) error {
	if len(result.Swaps) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO dex_pool_swaps (
		event_id, schema_version, chain_id, block_number, block_hash, block_time,
		transaction_hash, transaction_index, log_index, pool_address, factory_address,
		protocol, protocol_version, protocol_family, token0_address, token1_address,
		token0_symbol, token1_symbol, token0_decimals, token1_decimals,
		metadata_status, sender_address, recipient_address, amount0_delta_raw,
		amount1_delta_raw, sqrt_price_x96_raw, liquidity_raw, tick, parser_version,
		observed_at, is_canonical
	)`)
	if err != nil {
		return fmt.Errorf("prepare DEX swap batch: %w", err)
	}
	for _, swap := range result.Swaps {
		if err := batch.Append(
			swap.EventID(),
			swap.SchemaVersion,
			swap.ChainID,
			swap.BlockNumber,
			swap.BlockHash,
			swap.BlockTime,
			swap.TransactionHash,
			swap.TransactionIndex,
			swap.LogIndex,
			swap.PoolAddress,
			swap.FactoryAddress,
			swap.Protocol,
			swap.ProtocolVersion,
			swap.ProtocolFamily,
			swap.Token0Address,
			swap.Token1Address,
			swap.Token0Symbol,
			swap.Token1Symbol,
			swap.Token0Decimals,
			swap.Token1Decimals,
			swap.MetadataStatus,
			swap.SenderAddress,
			swap.RecipientAddress,
			swap.Amount0DeltaRaw,
			swap.Amount1DeltaRaw,
			swap.SqrtPriceX96Raw,
			swap.LiquidityRaw,
			swap.Tick,
			swap.ParserVersion,
			swap.ObservedAt,
			swap.IsCanonical,
		); err != nil {
			return fmt.Errorf("append DEX swap %s: %w", swap.EventID(), err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send DEX swap batch: %w", err)
	}
	return nil
}

func (s *EventStore) Close() error {
	return s.conn.Close()
}
