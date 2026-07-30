package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/basewatch/base-analytics/internal/gateway"
)

func (s *EventStore) TransactionTrace(
	ctx context.Context,
	chainID uint64,
	transactionHash string,
) (gateway.TransactionTrace, error) {
	var trace gateway.TransactionTrace
	row := s.conn.QueryRow(ctx, `
		SELECT
			trace_version, chain_id, block_number, block_hash, block_time,
			transaction_hash, transaction_index, wallet_address,
			transaction_target, root_selector, root_function, frame_count,
			max_depth, failed_call_count, delegatecall_count, pool_call_count,
			router_addresses, multicall_selectors, traced_at
		FROM transaction_trace_summaries FINAL
		WHERE chain_id = ? AND transaction_hash = ?
		ORDER BY traced_at DESC
		LIMIT 1`,
		chainID,
		transactionHash,
	)
	if err := row.Scan(
		&trace.TraceVersion,
		&trace.ChainID,
		&trace.BlockNumber,
		&trace.BlockHash,
		&trace.BlockTime,
		&trace.TransactionHash,
		&trace.TransactionIndex,
		&trace.WalletAddress,
		&trace.TransactionTarget,
		&trace.RootSelector,
		&trace.RootFunction,
		&trace.FrameCount,
		&trace.MaxDepth,
		&trace.FailedCallCount,
		&trace.DelegatecallCount,
		&trace.PoolCallCount,
		&trace.RouterAddresses,
		&trace.MulticallSelectors,
		&trace.TracedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return gateway.TransactionTrace{}, gateway.ErrNotFound
		}
		return gateway.TransactionTrace{}, fmt.Errorf("query transaction trace summary: %w", err)
	}
	rows, err := s.conn.Query(ctx, `
		SELECT
			trace_id, trace_address, parent_trace_address, depth, call_type,
			from_address, to_address, value_raw, gas_raw, gas_used_raw, input,
			output, function_selector, function_name, error, revert_reason,
			emitted_log_count, success, is_pool_call, is_router_call,
			is_multicall
		FROM transaction_call_traces FINAL
		WHERE trace_version = ? AND chain_id = ? AND transaction_hash = ?
		ORDER BY trace_address`,
		trace.TraceVersion,
		chainID,
		transactionHash,
	)
	if err != nil {
		return gateway.TransactionTrace{}, fmt.Errorf("query transaction call traces: %w", err)
	}
	defer rows.Close()
	trace.Calls = make([]gateway.TransactionCall, 0, trace.FrameCount)
	for rows.Next() {
		var call gateway.TransactionCall
		var success, isPool, isRouter, isMulticall uint8
		if err := rows.Scan(
			&call.TraceID,
			&call.TraceAddress,
			&call.ParentTraceAddress,
			&call.Depth,
			&call.CallType,
			&call.FromAddress,
			&call.ToAddress,
			&call.ValueRaw,
			&call.GasRaw,
			&call.GasUsedRaw,
			&call.Input,
			&call.Output,
			&call.FunctionSelector,
			&call.FunctionName,
			&call.Error,
			&call.RevertReason,
			&call.EmittedLogCount,
			&success,
			&isPool,
			&isRouter,
			&isMulticall,
		); err != nil {
			return gateway.TransactionTrace{}, fmt.Errorf("scan transaction call trace: %w", err)
		}
		call.Success = success == 1
		call.IsPoolCall = isPool == 1
		call.IsRouterCall = isRouter == 1
		call.IsMulticall = isMulticall == 1
		trace.Calls = append(trace.Calls, call)
	}
	if err := rows.Err(); err != nil {
		return gateway.TransactionTrace{}, fmt.Errorf("iterate transaction call traces: %w", err)
	}
	return trace, nil
}
