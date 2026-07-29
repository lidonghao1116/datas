package alerting

import (
	"context"
	"fmt"

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

func (s *PostgresOutbox) Close() {
	s.pool.Close()
}
