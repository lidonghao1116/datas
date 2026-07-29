package registry

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

const (
	selectorToken0   = "0x0dfe1681"
	selectorToken1   = "0xd21220a7"
	selectorFactory  = "0xc45a0155"
	selectorDecimals = "0x313ce567"
	selectorSymbol   = "0x95d89b41"
)

type ContractReader interface {
	Address(ctx context.Context, contract, selector string) (string, error)
	Uint8(ctx context.Context, contract, selector string) (uint8, error)
	String(ctx context.Context, contract, selector string) (string, error)
}

type BatchMetadataReader interface {
	PoolMetadata(ctx context.Context, contract string) (
		token0 string,
		token1 string,
		factory string,
		factoryKnown bool,
		err error,
	)
	TokenMetadata(ctx context.Context, contract string) (
		decimals uint8,
		decimalsKnown bool,
		symbol string,
		symbolKnown bool,
	)
}

type RPCReader struct {
	client      *rpc.Client
	timeout     time.Duration
	minInterval time.Duration

	rateMu   sync.Mutex
	nextCall time.Time
}

func OpenRPCReader(ctx context.Context, endpoint string, timeout time.Duration) (*RPCReader, error) {
	client, err := rpc.DialContext(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("dial registry RPC: %w", err)
	}
	return &RPCReader{
		client:      client,
		timeout:     timeout,
		minInterval: 250 * time.Millisecond,
	}, nil
}

func (r *RPCReader) PoolMetadata(
	ctx context.Context,
	contract string,
) (string, string, string, bool, error) {
	var token0Result, token1Result, factoryResult hexutil.Bytes
	batch := []rpc.BatchElem{
		{
			Method: "eth_call",
			Args:   []any{callArgs(contract, selectorToken0), "latest"},
			Result: &token0Result,
		},
		{
			Method: "eth_call",
			Args:   []any{callArgs(contract, selectorToken1), "latest"},
			Result: &token1Result,
		},
		{
			Method: "eth_call",
			Args:   []any{callArgs(contract, selectorFactory), "latest"},
			Result: &factoryResult,
		},
	}
	if err := r.batchCall(ctx, batch); err != nil {
		return "", "", "", false, err
	}
	if batch[0].Error != nil {
		return "", "", "", false, fmt.Errorf("read token0: %w", batch[0].Error)
	}
	if batch[1].Error != nil {
		return "", "", "", false, fmt.Errorf("read token1: %w", batch[1].Error)
	}
	token0, err := decodeAddressResult(token0Result)
	if err != nil {
		return "", "", "", false, fmt.Errorf("decode token0: %w", err)
	}
	token1, err := decodeAddressResult(token1Result)
	if err != nil {
		return "", "", "", false, fmt.Errorf("decode token1: %w", err)
	}
	if batch[2].Error != nil {
		return token0, token1, "", false, nil
	}
	factory, err := decodeAddressResult(factoryResult)
	if err != nil {
		return token0, token1, "", false, nil
	}
	return token0, token1, factory, true, nil
}

func (r *RPCReader) TokenMetadata(
	ctx context.Context,
	contract string,
) (uint8, bool, string, bool) {
	var decimalsResult, symbolResult hexutil.Bytes
	batch := []rpc.BatchElem{
		{
			Method: "eth_call",
			Args:   []any{callArgs(contract, selectorDecimals), "latest"},
			Result: &decimalsResult,
		},
		{
			Method: "eth_call",
			Args:   []any{callArgs(contract, selectorSymbol), "latest"},
			Result: &symbolResult,
		},
	}
	if err := r.batchCall(ctx, batch); err != nil {
		return 0, false, "", false
	}
	var decimals uint8
	decimalsKnown := false
	if batch[0].Error == nil {
		if value, err := decodeUint8Result(decimalsResult); err == nil {
			decimals = value
			decimalsKnown = true
		}
	}
	symbol := ""
	symbolKnown := false
	if batch[1].Error == nil {
		if value, err := decodeContractString(symbolResult); err == nil {
			symbol = strings.TrimSpace(value)
			symbolKnown = true
		}
	}
	return decimals, decimalsKnown, symbol, symbolKnown
}

func (r *RPCReader) Address(
	ctx context.Context,
	contract, selector string,
) (string, error) {
	result, err := r.call(ctx, contract, selector)
	if err != nil {
		return "", err
	}
	return decodeAddressResult(result)
}

func (r *RPCReader) Uint8(
	ctx context.Context,
	contract, selector string,
) (uint8, error) {
	result, err := r.call(ctx, contract, selector)
	if err != nil {
		return 0, err
	}
	return decodeUint8Result(result)
}

func (r *RPCReader) batchCall(ctx context.Context, batch []rpc.BatchElem) error {
	if err := r.waitForTurn(ctx); err != nil {
		return err
	}
	const maxAttempts = 6
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		for index := range batch {
			batch[index].Error = nil
		}
		requestCtx, cancel := context.WithTimeout(ctx, r.timeout)
		err := r.client.BatchCallContext(requestCtx, batch)
		cancel()
		lastErr = err
		rateLimited := isRateLimited(err)
		if err == nil {
			rateLimited = false
			for _, elem := range batch {
				if elem.Error != nil && isRateLimited(elem.Error) {
					rateLimited = true
					lastErr = elem.Error
					break
				}
			}
			if !rateLimited {
				return nil
			}
		}
		if !rateLimited || attempt == maxAttempts-1 {
			break
		}
		if err := waitContext(ctx, time.Duration(attempt+1)*300*time.Millisecond); err != nil {
			return err
		}
		if err := r.waitForTurn(ctx); err != nil {
			return err
		}
	}
	return fmt.Errorf("batch eth_call: %w", lastErr)
}

func callArgs(contract, selector string) map[string]string {
	return map[string]string{"to": contract, "data": selector}
}

func decodeAddressResult(result []byte) (string, error) {
	if len(result) < 32 {
		return "", fmt.Errorf("address call returned %d bytes", len(result))
	}
	address := common.BytesToAddress(result[len(result)-20:])
	if address == (common.Address{}) {
		return "", fmt.Errorf("address call returned zero address")
	}
	return strings.ToLower(address.Hex()), nil
}

func decodeUint8Result(result []byte) (uint8, error) {
	if len(result) < 32 {
		return 0, fmt.Errorf("uint8 call returned %d bytes", len(result))
	}
	value := new(big.Int).SetBytes(result[len(result)-32:])
	if value.BitLen() > 8 {
		return 0, fmt.Errorf("uint8 call returned out-of-range value %s", value)
	}
	return uint8(value.Uint64()), nil
}

func (r *RPCReader) String(
	ctx context.Context,
	contract, selector string,
) (string, error) {
	result, err := r.call(ctx, contract, selector)
	if err != nil {
		return "", err
	}
	value, err := decodeContractString(result)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(value), nil
}

func (r *RPCReader) call(
	ctx context.Context,
	contract, selector string,
) ([]byte, error) {
	if !common.IsHexAddress(contract) {
		return nil, fmt.Errorf("invalid contract address %q", contract)
	}

	const maxAttempts = 6
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := r.waitForTurn(ctx); err != nil {
			return nil, err
		}
		requestCtx, cancel := context.WithTimeout(ctx, r.timeout)
		var result hexutil.Bytes
		err := r.client.CallContext(
			requestCtx,
			&result,
			"eth_call",
			map[string]string{"to": contract, "data": selector},
			"latest",
		)
		cancel()
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !isRateLimited(err) || attempt == maxAttempts-1 {
			break
		}
		if err := waitContext(ctx, time.Duration(attempt+1)*300*time.Millisecond); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("eth_call %s on %s: %w", selector, contract, lastErr)
}

func (r *RPCReader) waitForTurn(ctx context.Context) error {
	r.rateMu.Lock()
	now := time.Now()
	wait := r.nextCall.Sub(now)
	if wait < 0 {
		wait = 0
	}
	r.nextCall = now.Add(wait).Add(r.minInterval)
	r.rateMu.Unlock()
	return waitContext(ctx, wait)
}

func isRateLimited(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "429") ||
		strings.Contains(message, "rate limit") ||
		strings.Contains(message, "over rate")
}

func waitContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func decodeContractString(result []byte) (string, error) {
	if len(result) == 32 {
		value := bytes.TrimRight(result, "\x00")
		if !utf8.Valid(value) {
			return "", fmt.Errorf("bytes32 string is not UTF-8")
		}
		return string(value), nil
	}
	if len(result) < 64 {
		return "", fmt.Errorf("string call returned %d bytes", len(result))
	}
	offset := new(big.Int).SetBytes(result[:32])
	if !offset.IsUint64() {
		return "", fmt.Errorf("string offset is out of range")
	}
	start := int(offset.Uint64())
	if start < 0 || start+32 > len(result) {
		return "", fmt.Errorf("string offset %d exceeds result length %d", start, len(result))
	}
	lengthValue := new(big.Int).SetBytes(result[start : start+32])
	if !lengthValue.IsUint64() {
		return "", fmt.Errorf("string length is out of range")
	}
	length := int(lengthValue.Uint64())
	start += 32
	if length < 0 || start+length > len(result) {
		return "", fmt.Errorf("string length %d exceeds result length %d", length, len(result))
	}
	value := result[start : start+length]
	if !utf8.Valid(value) {
		return "", fmt.Errorf("contract string is not UTF-8")
	}
	return string(value), nil
}

func (r *RPCReader) Close() {
	r.client.Close()
}
