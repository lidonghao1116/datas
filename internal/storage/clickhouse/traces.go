package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/basewatch/base-analytics/internal/traceanalytics"
)

func (s *EventStore) TraceCandidates(
	ctx context.Context,
	version string,
	chainID uint64,
	startBlock uint64,
	limit int,
) ([]traceanalytics.Candidate, error) {
	rows, err := s.conn.Query(ctx, `
		WITH
			swap_transactions AS (
				SELECT
					chain_id,
					block_number,
					argMax(block_hash, observed_at) AS latest_block_hash,
					argMax(block_time, observed_at) AS latest_block_time,
					transaction_hash,
					argMax(transaction_index, observed_at) AS latest_transaction_index,
					groupUniqArray(pool_address) AS pool_addresses
				FROM dex_pool_swaps FINAL
				WHERE chain_id = ?
				  AND block_number >= ?
				  AND is_canonical = 1
				GROUP BY chain_id, block_number, transaction_hash
			),
			transactions AS (
				SELECT
					chain_id,
					transaction_hash,
					argMax(from_address, observed_at) AS latest_from_address,
					argMax(to_address, observed_at) AS latest_to_address,
					argMax(input, observed_at) AS latest_input
				FROM raw_transactions FINAL
				WHERE chain_id = ? AND block_number >= ? AND is_canonical = 1
				GROUP BY chain_id, transaction_hash
			),
			latest_state AS (
				SELECT
					transaction_hash,
					argMax(status, attempted_at) AS latest_status,
					argMax(attempt_count, attempted_at) AS latest_attempt_count,
					argMax(next_retry_at, attempted_at) AS latest_next_retry_at
				FROM transaction_trace_sync_state
				WHERE trace_version = ? AND chain_id = ?
				GROUP BY transaction_hash
			),
			completed AS (
				SELECT transaction_hash
				FROM transaction_trace_summaries FINAL
				WHERE trace_version = ? AND chain_id = ?
			)
		SELECT
			s.chain_id,
			s.block_number,
			s.latest_block_hash,
			s.latest_block_time,
			s.transaction_hash,
			s.latest_transaction_index,
			t.latest_from_address,
			t.latest_to_address,
			t.latest_input,
			s.pool_addresses,
			st.latest_attempt_count
		FROM swap_transactions AS s
		INNER JOIN transactions AS t
			ON t.chain_id = s.chain_id
			AND t.transaction_hash = s.transaction_hash
		LEFT JOIN latest_state AS st
			ON st.transaction_hash = s.transaction_hash
		LEFT JOIN completed AS done
			ON done.transaction_hash = s.transaction_hash
		WHERE done.transaction_hash = ''
		  AND (
			st.transaction_hash = ''
			OR (
				st.latest_status = 'retry'
				AND st.latest_next_retry_at <= now64(3)
			)
		  )
		ORDER BY s.block_number, s.latest_transaction_index, s.transaction_hash
		LIMIT ?`,
		chainID,
		startBlock,
		chainID,
		startBlock,
		version,
		chainID,
		version,
		chainID,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query transaction trace candidates: %w", err)
	}
	defer rows.Close()
	candidates := make([]traceanalytics.Candidate, 0, limit)
	for rows.Next() {
		var candidate traceanalytics.Candidate
		if err := rows.Scan(
			&candidate.ChainID,
			&candidate.BlockNumber,
			&candidate.BlockHash,
			&candidate.BlockTime,
			&candidate.TransactionHash,
			&candidate.TransactionIndex,
			&candidate.WalletAddress,
			&candidate.TargetAddress,
			&candidate.Input,
			&candidate.PoolAddresses,
			&candidate.AttemptCount,
		); err != nil {
			return nil, fmt.Errorf("scan transaction trace candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate transaction trace candidates: %w", err)
	}
	return candidates, nil
}

func (s *EventStore) InsertTrace(
	ctx context.Context,
	result traceanalytics.Result,
) error {
	batch, err := s.conn.PrepareBatch(ctx, `
		INSERT INTO transaction_call_traces (
			trace_id, trace_version, chain_id, block_number, block_hash,
			block_time, transaction_hash, transaction_index, trace_address,
			parent_trace_address, depth, call_type, from_address, to_address,
			value_raw, gas_raw, gas_used_raw, input, output, function_selector,
			function_name, error, revert_reason, emitted_log_count, success,
			is_pool_call, is_router_call, is_multicall, traced_at
		)`)
	if err != nil {
		return fmt.Errorf("prepare transaction call trace batch: %w", err)
	}
	for _, call := range result.Calls {
		if err := batch.Append(
			call.TraceID,
			traceanalytics.Version,
			result.ChainID,
			result.BlockNumber,
			result.BlockHash,
			result.BlockTime,
			result.TransactionHash,
			result.TransactionIndex,
			call.TraceAddress,
			call.ParentTraceAddress,
			call.Depth,
			call.CallType,
			call.FromAddress,
			call.ToAddress,
			call.ValueRaw,
			call.GasRaw,
			call.GasUsedRaw,
			call.Input,
			call.Output,
			call.FunctionSelector,
			call.FunctionName,
			call.Error,
			call.RevertReason,
			call.EmittedLogCount,
			boolToUInt8(call.Success),
			boolToUInt8(call.IsPoolCall),
			boolToUInt8(call.IsRouterCall),
			boolToUInt8(call.IsMulticall),
			result.TracedAt,
		); err != nil {
			return fmt.Errorf("append transaction call trace %s: %w", call.TraceID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send transaction call traces: %w", err)
	}
	if err := s.conn.Exec(ctx, `
		INSERT INTO transaction_trace_summaries (
			trace_version, chain_id, block_number, block_hash, block_time,
			transaction_hash, transaction_index, wallet_address,
			transaction_target, root_selector, root_function, frame_count,
			max_depth, failed_call_count, delegatecall_count, pool_call_count,
			router_addresses, multicall_selectors, raw_trace_json, traced_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		traceanalytics.Version,
		result.ChainID,
		result.BlockNumber,
		result.BlockHash,
		result.BlockTime,
		result.TransactionHash,
		result.TransactionIndex,
		result.WalletAddress,
		result.TargetAddress,
		result.RootSelector,
		result.RootFunction,
		result.FrameCount,
		result.MaxDepth,
		result.FailedCallCount,
		result.DelegatecallCount,
		result.PoolCallCount,
		result.RouterAddresses,
		result.MulticallSelectors,
		string(result.RawTrace),
		result.TracedAt,
	); err != nil {
		return fmt.Errorf("insert transaction trace summary: %w", err)
	}
	return s.insertTraceState(
		ctx,
		result.ChainID,
		result.BlockNumber,
		result.TransactionHash,
		traceanalytics.Version,
		result.AttemptCount+1,
		"completed",
		"",
		time.Unix(0, 0).UTC(),
		result.TracedAt,
	)
}

func (s *EventStore) RecordTraceFailure(
	ctx context.Context,
	candidate traceanalytics.Candidate,
	version string,
	attemptCount uint32,
	nextRetryAt time.Time,
	status string,
	lastError string,
) error {
	return s.insertTraceState(
		ctx,
		candidate.ChainID,
		candidate.BlockNumber,
		candidate.TransactionHash,
		version,
		attemptCount,
		status,
		lastError,
		nextRetryAt,
		time.Now().UTC(),
	)
}

func (s *EventStore) insertTraceState(
	ctx context.Context,
	chainID uint64,
	blockNumber uint64,
	transactionHash string,
	version string,
	attemptCount uint32,
	status string,
	lastError string,
	nextRetryAt time.Time,
	attemptedAt time.Time,
) error {
	if err := s.conn.Exec(ctx, `
		INSERT INTO transaction_trace_sync_state (
			trace_version, chain_id, block_number, transaction_hash, status,
			attempt_count, last_error, next_retry_at, attempted_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		version,
		chainID,
		blockNumber,
		transactionHash,
		status,
		attemptCount,
		lastError,
		nextRetryAt,
		attemptedAt,
	); err != nil {
		return fmt.Errorf("insert transaction trace sync state: %w", err)
	}
	return nil
}
