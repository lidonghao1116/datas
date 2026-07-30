package checkpoint

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store interface {
	Load(ctx context.Context, pipeline string, chainID uint64) (Point, bool, error)
	Header(ctx context.Context, pipeline string, chainID, blockNumber uint64) (Point, bool, error)
	Range(ctx context.Context, pipeline string, chainID, fromBlock, toBlock uint64) ([]Point, error)
	Save(ctx context.Context, pipeline string, chainID, blockNumber uint64, blockHash string) error
	Rewind(ctx context.Context, pipeline string, chainID uint64, ancestor Point) error
}

type Point struct {
	BlockNumber uint64
	BlockHash   string
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

func (s *PostgresStore) Load(ctx context.Context, pipeline string, chainID uint64) (Point, bool, error) {
	var blockNumber int64
	var blockHash string
	err := s.pool.QueryRow(
		ctx,
		`SELECT block_number, block_hash
		 FROM ingestion_checkpoints WHERE pipeline = $1 AND chain_id = $2`,
		pipeline,
		int64(chainID),
	).Scan(&blockNumber, &blockHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return Point{}, false, nil
	}
	if err != nil {
		return Point{}, false, fmt.Errorf("load checkpoint: %w", err)
	}
	return Point{BlockNumber: uint64(blockNumber), BlockHash: blockHash}, true, nil
}

func (s *PostgresStore) Header(
	ctx context.Context,
	pipeline string,
	chainID, blockNumber uint64,
) (Point, bool, error) {
	var blockHash string
	err := s.pool.QueryRow(
		ctx,
		`SELECT block_hash
		 FROM canonical_block_headers
		 WHERE pipeline = $1 AND chain_id = $2 AND block_number = $3`,
		pipeline,
		int64(chainID),
		int64(blockNumber),
	).Scan(&blockHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return Point{}, false, nil
	}
	if err != nil {
		return Point{}, false, fmt.Errorf("load canonical header %d: %w", blockNumber, err)
	}
	return Point{BlockNumber: blockNumber, BlockHash: blockHash}, true, nil
}

func (s *PostgresStore) Range(
	ctx context.Context,
	pipeline string,
	chainID, fromBlock, toBlock uint64,
) ([]Point, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT block_number, block_hash
		 FROM canonical_block_headers
		 WHERE pipeline = $1 AND chain_id = $2
		   AND block_number BETWEEN $3 AND $4
		 ORDER BY block_number`,
		pipeline,
		int64(chainID),
		int64(fromBlock),
		int64(toBlock),
	)
	if err != nil {
		return nil, fmt.Errorf("load canonical header range: %w", err)
	}
	defer rows.Close()
	points := make([]Point, 0, toBlock-fromBlock+1)
	for rows.Next() {
		var blockNumber int64
		var blockHash string
		if err := rows.Scan(&blockNumber, &blockHash); err != nil {
			return nil, fmt.Errorf("scan canonical header: %w", err)
		}
		points = append(points, Point{BlockNumber: uint64(blockNumber), BlockHash: blockHash})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate canonical headers: %w", err)
	}
	return points, nil
}

func (s *PostgresStore) Save(
	ctx context.Context,
	pipeline string,
	chainID, blockNumber uint64,
	blockHash string,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin checkpoint transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO canonical_block_headers (
			pipeline, chain_id, block_number, block_hash, observed_at
		) VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (pipeline, chain_id, block_number)
		DO UPDATE SET block_hash = EXCLUDED.block_hash,
		              observed_at = EXCLUDED.observed_at`,
		pipeline,
		int64(chainID),
		int64(blockNumber),
		blockHash,
	); err != nil {
		return fmt.Errorf("save canonical header for block %d: %w", blockNumber, err)
	}
	_, err = tx.Exec(
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
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit checkpoint for block %d: %w", blockNumber, err)
	}
	return nil
}

func (s *PostgresStore) Rewind(
	ctx context.Context,
	pipeline string,
	chainID uint64,
	ancestor Point,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin checkpoint rewind: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(
		ctx,
		`DELETE FROM canonical_block_headers
		 WHERE pipeline = $1 AND chain_id = $2 AND block_number > $3`,
		pipeline,
		int64(chainID),
		int64(ancestor.BlockNumber),
	); err != nil {
		return fmt.Errorf("remove orphaned canonical headers: %w", err)
	}
	if _, err := tx.Exec(
		ctx,
		`UPDATE ingestion_checkpoints
		 SET block_number = $3, block_hash = $4, updated_at = now()
		 WHERE pipeline = $1 AND chain_id = $2`,
		pipeline,
		int64(chainID),
		int64(ancestor.BlockNumber),
		ancestor.BlockHash,
	); err != nil {
		return fmt.Errorf("rewind checkpoint to block %d: %w", ancestor.BlockNumber, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit checkpoint rewind: %w", err)
	}
	return nil
}

func (s *PostgresStore) Close() {
	s.pool.Close()
}
