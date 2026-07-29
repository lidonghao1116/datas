package checkpoint

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store interface {
	Load(ctx context.Context, pipeline string, chainID uint64) (uint64, bool, error)
	Save(ctx context.Context, pipeline string, chainID, blockNumber uint64, blockHash string) error
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func OpenPostgres(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL checkpoint store: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL checkpoint store: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Load(ctx context.Context, pipeline string, chainID uint64) (uint64, bool, error) {
	var blockNumber int64
	err := s.pool.QueryRow(
		ctx,
		`SELECT block_number FROM ingestion_checkpoints WHERE pipeline = $1 AND chain_id = $2`,
		pipeline,
		int64(chainID),
	).Scan(&blockNumber)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("load checkpoint: %w", err)
	}
	return uint64(blockNumber), true, nil
}

func (s *PostgresStore) Save(
	ctx context.Context,
	pipeline string,
	chainID, blockNumber uint64,
	blockHash string,
) error {
	_, err := s.pool.Exec(
		ctx,
		`INSERT INTO ingestion_checkpoints (pipeline, chain_id, block_number, block_hash, updated_at)
		 VALUES ($1, $2, $3, $4, now())
		 ON CONFLICT (pipeline, chain_id)
		 DO UPDATE SET block_number = EXCLUDED.block_number,
		               block_hash = EXCLUDED.block_hash,
		               updated_at = EXCLUDED.updated_at
		 WHERE ingestion_checkpoints.block_number <= EXCLUDED.block_number`,
		pipeline,
		int64(chainID),
		int64(blockNumber),
		blockHash,
	)
	if err != nil {
		return fmt.Errorf("save checkpoint for block %d: %w", blockNumber, err)
	}
	return nil
}

func (s *PostgresStore) Close() {
	s.pool.Close()
}
