package alerting

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresOutbox struct {
	pool *pgxpool.Pool
}

func OpenPostgresOutbox(ctx context.Context, dsn string) (*PostgresOutbox, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open alert outbox: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping alert outbox: %w", err)
	}
	return &PostgresOutbox{pool: pool}, nil
}

func (s *PostgresOutbox) InsertAlerts(
	ctx context.Context,
	alerts []Alert,
) (int, error) {
	if len(alerts) == 0 {
		return 0, nil
	}
	transaction, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin alert outbox transaction: %w", err)
	}
	defer transaction.Rollback(ctx)

	inserted := 0
	for _, alert := range alerts {
		tag, err := transaction.Exec(
			ctx,
			`INSERT INTO alert_outbox (
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
			return 0, fmt.Errorf("insert alert %s: %w", alert.Key, err)
		}
		inserted += int(tag.RowsAffected())
	}
	if err := transaction.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit alert outbox transaction: %w", err)
	}
	return inserted, nil
}

func (s *PostgresOutbox) ClaimDeliveries(
	ctx context.Context,
	workerID string,
	limit int,
	lease time.Duration,
) ([]Delivery, error) {
	rows, err := s.pool.Query(
		ctx,
		`WITH candidates AS (
			SELECT alert_key
			  FROM alert_outbox
			 WHERE (
					(status IN ('pending', 'failed') AND next_attempt_at <= now())
					OR
					(status = 'processing' AND locked_at <= now() - $2::interval)
			 )
			 ORDER BY next_attempt_at, created_at
			 FOR UPDATE SKIP LOCKED
			 LIMIT $1
		)
		UPDATE alert_outbox AS target
		   SET status = 'processing',
		       attempts = target.attempts + 1,
		       locked_at = now(),
		       locked_by = $3,
		       updated_at = now()
		  FROM candidates
		 WHERE target.alert_key = candidates.alert_key
		RETURNING
			target.alert_key, target.alert_type, target.severity,
			target.chain_id, target.block_number, target.transaction_hash,
			target.token_address, target.token_symbol, target.title,
			target.payload, target.attempts, target.created_at`,
		limit,
		lease.String(),
		workerID,
	)
	if err != nil {
		return nil, fmt.Errorf("claim alert deliveries: %w", err)
	}
	defer rows.Close()

	deliveries := make([]Delivery, 0, limit)
	for rows.Next() {
		var delivery Delivery
		if err := rows.Scan(
			&delivery.Key,
			&delivery.Type,
			&delivery.Severity,
			&delivery.ChainID,
			&delivery.BlockNumber,
			&delivery.TransactionHash,
			&delivery.TokenAddress,
			&delivery.TokenSymbol,
			&delivery.Title,
			&delivery.Payload,
			&delivery.Attempt,
			&delivery.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan alert delivery: %w", err)
		}
		deliveries = append(deliveries, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate alert deliveries: %w", err)
	}
	return deliveries, nil
}

func (s *PostgresOutbox) MarkDelivered(
	ctx context.Context,
	alertKey, workerID string,
) error {
	tag, err := s.pool.Exec(
		ctx,
		`UPDATE alert_outbox
		    SET status = 'delivered',
		        delivered_at = now(),
		        locked_at = NULL,
		        locked_by = '',
		        last_error = '',
		        updated_at = now()
		  WHERE alert_key = $1
		    AND status = 'processing'
		    AND locked_by = $2`,
		alertKey,
		workerID,
	)
	if err != nil {
		return fmt.Errorf("mark alert %s delivered: %w", alertKey, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("delivery lease lost for alert %s", alertKey)
	}
	return nil
}

func (s *PostgresOutbox) MarkFailed(
	ctx context.Context,
	alertKey, workerID, lastError string,
	nextAttemptAt time.Time,
	deadLetter bool,
) error {
	status := "failed"
	if deadLetter {
		status = "dead_letter"
	}
	if len(lastError) > 2000 {
		lastError = lastError[:2000]
	}
	tag, err := s.pool.Exec(
		ctx,
		`UPDATE alert_outbox
		    SET status = $3,
		        next_attempt_at = $4,
		        locked_at = NULL,
		        locked_by = '',
		        last_error = $5,
		        updated_at = now()
		  WHERE alert_key = $1
		    AND status = 'processing'
		    AND locked_by = $2`,
		alertKey,
		workerID,
		status,
		nextAttemptAt,
		lastError,
	)
	if err != nil {
		return fmt.Errorf("mark alert %s failed: %w", alertKey, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("delivery lease lost for alert %s", alertKey)
	}
	return nil
}

func (s *PostgresOutbox) Close() {
	s.pool.Close()
}
