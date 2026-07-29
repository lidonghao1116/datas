package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/basewatch/base-analytics/internal/marketdata"
)

type Store interface {
	GetFactory(ctx context.Context, chainID uint64, address string) (Factory, bool, error)
	GetPool(ctx context.Context, chainID uint64, address string) (Pool, bool, error)
	UpsertPool(ctx context.Context, pool Pool) error
	GetToken(ctx context.Context, chainID uint64, address string) (Token, bool, error)
	UpsertToken(ctx context.Context, token Token) error
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func OpenPostgres(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL registry: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL registry: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) GetFactory(
	ctx context.Context,
	chainID uint64,
	address string,
) (Factory, bool, error) {
	var factory Factory
	err := s.pool.QueryRow(
		ctx,
		`SELECT chain_id, factory_address, protocol, protocol_version,
		        protocol_family, is_verified, source
		   FROM dex_factories
		  WHERE chain_id = $1 AND factory_address = $2`,
		int64(chainID),
		address,
	).Scan(
		&factory.ChainID,
		&factory.Address,
		&factory.Protocol,
		&factory.ProtocolVersion,
		&factory.ProtocolFamily,
		&factory.Verified,
		&factory.Source,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Factory{}, false, nil
	}
	if err != nil {
		return Factory{}, false, fmt.Errorf("get DEX factory %s: %w", address, err)
	}
	return factory, true, nil
}

func (s *PostgresStore) GetPool(
	ctx context.Context,
	chainID uint64,
	address string,
) (Pool, bool, error) {
	var pool Pool
	err := s.pool.QueryRow(
		ctx,
		`SELECT chain_id, pool_address, factory_address, protocol,
		        protocol_version, protocol_family, token0_address,
		        token1_address, discovered_block, observed_at
		   FROM dex_pools
		  WHERE chain_id = $1 AND pool_address = $2`,
		int64(chainID),
		address,
	).Scan(
		&pool.ChainID,
		&pool.Address,
		&pool.FactoryAddress,
		&pool.Protocol,
		&pool.ProtocolVersion,
		&pool.ProtocolFamily,
		&pool.Token0Address,
		&pool.Token1Address,
		&pool.DiscoveredBlock,
		&pool.ObservedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Pool{}, false, nil
	}
	if err != nil {
		return Pool{}, false, fmt.Errorf("get DEX pool %s: %w", address, err)
	}
	return pool, true, nil
}

func (s *PostgresStore) UpsertPool(ctx context.Context, pool Pool) error {
	_, err := s.pool.Exec(
		ctx,
		`INSERT INTO dex_pools (
			chain_id, pool_address, factory_address, protocol,
			protocol_version, protocol_family, token0_address,
			token1_address, discovered_block, observed_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now())
		ON CONFLICT (chain_id, pool_address) DO UPDATE SET
			factory_address = EXCLUDED.factory_address,
			protocol = EXCLUDED.protocol,
			protocol_version = EXCLUDED.protocol_version,
			protocol_family = EXCLUDED.protocol_family,
			token0_address = EXCLUDED.token0_address,
			token1_address = EXCLUDED.token1_address,
			updated_at = now()`,
		int64(pool.ChainID),
		pool.Address,
		pool.FactoryAddress,
		pool.Protocol,
		pool.ProtocolVersion,
		pool.ProtocolFamily,
		pool.Token0Address,
		pool.Token1Address,
		int64(pool.DiscoveredBlock),
		pool.ObservedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert DEX pool %s: %w", pool.Address, err)
	}
	return nil
}

func (s *PostgresStore) GetToken(
	ctx context.Context,
	chainID uint64,
	address string,
) (Token, bool, error) {
	var token Token
	var decimals int16
	err := s.pool.QueryRow(
		ctx,
		`SELECT chain_id, token_address, symbol, decimals,
		        symbol_known, decimals_known, observed_at
		   FROM token_metadata
		  WHERE chain_id = $1 AND token_address = $2`,
		int64(chainID),
		address,
	).Scan(
		&token.ChainID,
		&token.Address,
		&token.Symbol,
		&decimals,
		&token.SymbolKnown,
		&token.DecimalsKnown,
		&token.ObservedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Token{}, false, nil
	}
	if err != nil {
		return Token{}, false, fmt.Errorf("get token %s: %w", address, err)
	}
	token.Decimals = uint8(decimals)
	return token, true, nil
}

func (s *PostgresStore) UpsertToken(ctx context.Context, token Token) error {
	_, err := s.pool.Exec(
		ctx,
		`INSERT INTO token_metadata (
			chain_id, token_address, symbol, decimals, symbol_known,
			decimals_known, observed_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,now())
		ON CONFLICT (chain_id, token_address) DO UPDATE SET
			symbol = EXCLUDED.symbol,
			decimals = EXCLUDED.decimals,
			symbol_known = EXCLUDED.symbol_known,
			decimals_known = EXCLUDED.decimals_known,
			updated_at = now()`,
		int64(token.ChainID),
		token.Address,
		token.Symbol,
		int16(token.Decimals),
		token.SymbolKnown,
		token.DecimalsKnown,
		token.ObservedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert token %s: %w", token.Address, err)
	}
	return nil
}

func (s *PostgresStore) ListMarketTokens(
	ctx context.Context,
	chainID uint64,
	afterAddress string,
	limit int,
) ([]marketdata.Token, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT token_address
		   FROM token_metadata
		  WHERE chain_id = $1
		    AND token_address > $2
		  ORDER BY token_address
		  LIMIT $3`,
		int64(chainID),
		afterAddress,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list market data tokens: %w", err)
	}
	defer rows.Close()

	tokens := make([]marketdata.Token, 0, limit)
	for rows.Next() {
		var address string
		if err := rows.Scan(&address); err != nil {
			return nil, fmt.Errorf("scan market data token: %w", err)
		}
		tokens = append(tokens, marketdata.Token{ChainID: chainID, Address: address})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate market data tokens: %w", err)
	}
	return tokens, nil
}

func (s *PostgresStore) Close() {
	s.pool.Close()
}
