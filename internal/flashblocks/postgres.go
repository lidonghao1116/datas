package flashblocks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/basewatch/base-analytics/internal/alerting"
)

type PostgresStateStore struct {
	pool *pgxpool.Pool
}

func OpenPostgresStateStore(
	ctx context.Context,
	dsn string,
) (*PostgresStateStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open Flashblocks state store: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping Flashblocks state store: %w", err)
	}
	return &PostgresStateStore{pool: pool}, nil
}

func (s *PostgresStateStore) InsertPending(
	ctx context.Context,
	preconfirmation Preconfirmation,
	alerts []alerting.Alert,
) (intentionallyInserted bool, err error) {
	transaction, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin preconfirmation transaction: %w", err)
	}
	defer transaction.Rollback(ctx)
	tag, err := transaction.Exec(ctx, `
		INSERT INTO flashblock_preconfirmations (
			preconfirmation_key, chain_id, transaction_hash, log_index,
			pool_address, preconfirmed_block, preconfirmed_hash, alert_keys,
			payload, observed_at, expires_at, next_check_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$10)
		ON CONFLICT (preconfirmation_key) DO NOTHING`,
		preconfirmation.Key,
		int64(preconfirmation.ChainID),
		preconfirmation.TransactionHash,
		int32(preconfirmation.LogIndex),
		preconfirmation.PoolAddress,
		int64(preconfirmation.BlockNumber),
		preconfirmation.BlockHash,
		preconfirmation.AlertKeys,
		preconfirmation.Payload,
		preconfirmation.ObservedAt,
		preconfirmation.ExpiresAt,
	)
	if err != nil {
		return false, fmt.Errorf("insert preconfirmation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	for _, alert := range alerts {
		if err := insertAlert(ctx, transaction, alert); err != nil {
			return false, err
		}
	}
	if err := transaction.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit preconfirmation transaction: %w", err)
	}
	return true, nil
}

func (s *PostgresStateStore) PendingByKey(
	ctx context.Context,
	key string,
) (Reconciliation, bool, error) {
	var item Reconciliation
	err := s.pool.QueryRow(ctx, `
		SELECT
			preconfirmation_key, chain_id, transaction_hash, log_index,
			pool_address, preconfirmed_block, preconfirmed_hash, observed_at,
			expires_at, alert_keys, payload, attempt_count
		FROM flashblock_preconfirmations
		WHERE preconfirmation_key = $1 AND status = 'pending'`,
		key,
	).Scan(
		&item.Key,
		&item.ChainID,
		&item.TransactionHash,
		&item.LogIndex,
		&item.PoolAddress,
		&item.BlockNumber,
		&item.BlockHash,
		&item.ObservedAt,
		&item.ExpiresAt,
		&item.AlertKeys,
		&item.Payload,
		&item.AttemptCount,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reconciliation{}, false, nil
	}
	if err != nil {
		return Reconciliation{}, false, fmt.Errorf("query pending preconfirmation %s: %w", key, err)
	}
	return item, true, nil
}

func (s *PostgresStateStore) PendingReconciliations(
	ctx context.Context,
	limit int,
) ([]Reconciliation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			preconfirmation_key, chain_id, transaction_hash, log_index,
			pool_address, preconfirmed_block, preconfirmed_hash, observed_at,
			expires_at, alert_keys, payload, attempt_count
		FROM flashblock_preconfirmations
		WHERE status = 'pending' AND next_check_at <= now()
		ORDER BY next_check_at, observed_at
		LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query pending preconfirmations: %w", err)
	}
	defer rows.Close()
	items := make([]Reconciliation, 0, limit)
	for rows.Next() {
		var item Reconciliation
		if err := rows.Scan(
			&item.Key,
			&item.ChainID,
			&item.TransactionHash,
			&item.LogIndex,
			&item.PoolAddress,
			&item.BlockNumber,
			&item.BlockHash,
			&item.ObservedAt,
			&item.ExpiresAt,
			&item.AlertKeys,
			&item.Payload,
			&item.AttemptCount,
		); err != nil {
			return nil, fmt.Errorf("scan pending preconfirmation: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending preconfirmations: %w", err)
	}
	return items, nil
}

func (s *PostgresStateStore) DeferReconciliation(
	ctx context.Context,
	key string,
	nextCheckAt time.Time,
	lastError string,
) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE flashblock_preconfirmations
		SET next_check_at = $2,
			attempt_count = attempt_count + 1,
			last_error = $3,
			updated_at = now()
		WHERE preconfirmation_key = $1 AND status = 'pending'`,
		key,
		nextCheckAt,
		lastError,
	)
	if err != nil {
		return fmt.Errorf("defer preconfirmation reconciliation: %w", err)
	}
	return nil
}

func (s *PostgresStateStore) Resolve(
	ctx context.Context,
	preconfirmation Reconciliation,
	status string,
	resolvedAt time.Time,
	blockNumber uint64,
	blockHash string,
	alert alerting.Alert,
) error {
	transaction, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin preconfirmation resolution: %w", err)
	}
	defer transaction.Rollback(ctx)
	tag, err := transaction.Exec(ctx, `
		UPDATE flashblock_preconfirmations
		SET status = $2,
			canonical_block = NULLIF($3, 0),
			canonical_block_hash = NULLIF($4, ''),
			resolved_at = $5,
			updated_at = now()
		WHERE preconfirmation_key = $1 AND status = 'pending'`,
		preconfirmation.Key,
		status,
		int64(blockNumber),
		blockHash,
		resolvedAt,
	)
	if err != nil {
		return fmt.Errorf("resolve preconfirmation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	if err := insertAlert(ctx, transaction, alert); err != nil {
		return err
	}
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit preconfirmation resolution: %w", err)
	}
	return nil
}

func insertAlert(
	ctx context.Context,
	transaction pgx.Tx,
	alert alerting.Alert,
) error {
	_, err := transaction.Exec(ctx, `
		INSERT INTO alert_outbox (
			alert_key, alert_type, severity, chain_id, block_number,
			transaction_hash, token_address, token_symbol, title, payload
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (alert_key) DO NOTHING`,
		alert.Key,
		alert.Type,
		alert.Severity,
		int64(alert.ChainID),
		int64(alert.BlockNumber),
		alert.TransactionHash,
		alert.TokenAddress,
		alert.TokenSymbol,
		alert.Title,
		alert.Payload,
	)
	if err != nil {
		return fmt.Errorf("insert preconfirmation alert %s: %w", alert.Key, err)
	}
	return nil
}

func (s *PostgresStateStore) Close() {
	s.pool.Close()
}
