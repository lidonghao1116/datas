package gateway

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresAlertStore struct {
	pool *pgxpool.Pool
}

func OpenPostgresAlertStore(
	ctx context.Context,
	dsn string,
) (*PostgresAlertStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open API alert store: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping API alert store: %w", err)
	}
	return &PostgresAlertStore{pool: pool}, nil
}

func (s *PostgresAlertStore) RecentAlerts(
	ctx context.Context,
	filter AlertFilter,
) ([]Alert, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT
			alert_key, alert_type, severity, status, chain_id, block_number,
			transaction_hash, token_address, token_symbol, title, payload,
			attempts, created_at, updated_at, delivered_at
		   FROM alert_outbox
		  WHERE ($1 = '' OR status = $1)
		    AND ($2 = '' OR severity = $2)
		    AND ($3 = '' OR alert_type = $3)
		  ORDER BY created_at DESC, alert_key DESC
		  LIMIT $4`,
		filter.Status,
		filter.Severity,
		filter.Type,
		filter.Limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query recent alerts: %w", err)
	}
	defer rows.Close()
	return scanAlerts(rows)
}

func (s *PostgresAlertStore) AlertsAfter(
	ctx context.Context,
	cursor AlertCursor,
	limit int,
) ([]Alert, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT
			alert_key, alert_type, severity, status, chain_id, block_number,
			transaction_hash, token_address, token_symbol, title, payload,
			attempts, created_at, updated_at, delivered_at
		   FROM alert_outbox
		  WHERE (created_at, alert_key) > ($1, $2)
		  ORDER BY created_at, alert_key
		  LIMIT $3`,
		cursor.CreatedAt,
		cursor.Key,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query alerts after cursor: %w", err)
	}
	defer rows.Close()
	return scanAlerts(rows)
}

type alertRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

func scanAlerts(rows alertRows) ([]Alert, error) {
	alerts := make([]Alert, 0)
	for rows.Next() {
		var alert Alert
		if err := rows.Scan(
			&alert.Key,
			&alert.Type,
			&alert.Severity,
			&alert.Status,
			&alert.ChainID,
			&alert.BlockNumber,
			&alert.TransactionHash,
			&alert.TokenAddress,
			&alert.TokenSymbol,
			&alert.Title,
			&alert.Payload,
			&alert.Attempts,
			&alert.CreatedAt,
			&alert.UpdatedAt,
			&alert.DeliveredAt,
		); err != nil {
			return nil, fmt.Errorf("scan alert: %w", err)
		}
		alerts = append(alerts, alert)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate alerts: %w", err)
	}
	return alerts, nil
}

func (s *PostgresAlertStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *PostgresAlertStore) Close() {
	s.pool.Close()
}
