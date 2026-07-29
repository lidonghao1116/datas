package base

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/basewatch/base-analytics/internal/domain"
)

type Head struct {
	Number     hexutil.Uint64 `json:"number"`
	Hash       common.Hash    `json:"hash"`
	ParentHash common.Hash    `json:"parentHash"`
}

type Client struct {
	httpURL string
	wssURL  string
	chainID uint64
	timeout time.Duration
}

func NewClient(httpURL, wssURL string, chainID uint64, timeout time.Duration) *Client {
	return &Client{
		httpURL: httpURL,
		wssURL:  wssURL,
		chainID: chainID,
		timeout: timeout,
	}
}

func (c *Client) LatestBlockNumber(ctx context.Context) (uint64, error) {
	client, err := rpc.DialContext(ctx, c.httpURL)
	if err != nil {
		return 0, fmt.Errorf("dial Base HTTP RPC: %w", err)
	}
	defer client.Close()

	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var result hexutil.Uint64
	if err := client.CallContext(requestCtx, &result, "eth_blockNumber"); err != nil {
		return 0, fmt.Errorf("eth_blockNumber: %w", err)
	}
	return uint64(result), nil
}

func (c *Client) FetchBlock(ctx context.Context, number uint64) (domain.RawBlockEnvelope, error) {
	client, err := rpc.DialContext(ctx, c.httpURL)
	if err != nil {
		return domain.RawBlockEnvelope{}, fmt.Errorf("dial Base HTTP RPC: %w", err)
	}
	defer client.Close()

	blockNumber := hexutil.EncodeUint64(number)
	var rawBlock json.RawMessage
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	if err := client.CallContext(
		requestCtx,
		&rawBlock,
		"eth_getBlockByNumber",
		blockNumber,
		true,
	); err != nil {
		cancel()
		return domain.RawBlockEnvelope{}, fmt.Errorf("eth_getBlockByNumber for block %d: %w", number, err)
	}
	cancel()
	if len(rawBlock) == 0 || string(rawBlock) == "null" {
		return domain.RawBlockEnvelope{}, fmt.Errorf("block %d was not found", number)
	}

	var metadata struct {
		Number       hexutil.Uint64    `json:"number"`
		Hash         common.Hash       `json:"hash"`
		ParentHash   common.Hash       `json:"parentHash"`
		Timestamp    hexutil.Uint64    `json:"timestamp"`
		Transactions []json.RawMessage `json:"transactions"`
	}
	if err := json.Unmarshal(rawBlock, &metadata); err != nil {
		return domain.RawBlockEnvelope{}, fmt.Errorf("decode block %d metadata: %w", number, err)
	}
	if uint64(metadata.Number) != number {
		return domain.RawBlockEnvelope{}, fmt.Errorf("requested block %d but RPC returned %d", number, metadata.Number)
	}

	rawReceipts, err := c.fetchReceipts(ctx, client, blockNumber, metadata.Transactions)
	if err != nil {
		return domain.RawBlockEnvelope{}, fmt.Errorf("fetch receipts for block %d: %w", number, err)
	}
	if len(metadata.Transactions) != len(rawReceipts) {
		return domain.RawBlockEnvelope{}, fmt.Errorf(
			"block %d transaction/receipt count mismatch: %d != %d",
			number,
			len(metadata.Transactions),
			len(rawReceipts),
		)
	}

	envelope := domain.RawBlockEnvelope{
		SchemaVersion: domain.RawBlockSchemaVersion,
		ChainID:       c.chainID,
		BlockNumber:   number,
		BlockHash:     metadata.Hash.Hex(),
		ParentHash:    metadata.ParentHash.Hex(),
		BlockTime:     time.Unix(int64(metadata.Timestamp), 0).UTC(),
		ObservedAt:    time.Now().UTC(),
		Provider:      c.httpURL,
		Block:         rawBlock,
		Receipts:      rawReceipts,
	}
	if err := envelope.Validate(); err != nil {
		return domain.RawBlockEnvelope{}, fmt.Errorf("validate block %d: %w", number, err)
	}
	return envelope, nil
}

func (c *Client) fetchReceipts(
	ctx context.Context,
	client *rpc.Client,
	blockNumber string,
	transactions []json.RawMessage,
) ([]json.RawMessage, error) {
	if len(transactions) == 0 {
		return []json.RawMessage{}, nil
	}

	var receipts []json.RawMessage
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	err := client.CallContext(requestCtx, &receipts, "eth_getBlockReceipts", blockNumber)
	cancel()
	if err == nil {
		return receipts, nil
	}
	if !methodUnsupported(err) {
		return nil, fmt.Errorf("eth_getBlockReceipts: %w", err)
	}

	// The public Base RPC endpoint accepts at most 10 calls per JSON-RPC batch
	// and rate-limits bursts. Keep requests within both constraints.
	const (
		batchSize      = 10
		batchInterval  = 250 * time.Millisecond
		maxBatchTries  = 5
		retryBaseDelay = 500 * time.Millisecond
	)
	receipts = make([]json.RawMessage, len(transactions))
	for start := 0; start < len(transactions); start += batchSize {
		if start > 0 {
			if err := waitForRPC(ctx, batchInterval); err != nil {
				return nil, err
			}
		}
		end := min(start+batchSize, len(transactions))
		batch := make([]rpc.BatchElem, 0, end-start)
		for index := start; index < end; index++ {
			var transaction struct {
				Hash string `json:"hash"`
			}
			if err := json.Unmarshal(transactions[index], &transaction); err != nil {
				return nil, fmt.Errorf("decode transaction %d: %w", index, err)
			}
			if transaction.Hash == "" {
				return nil, fmt.Errorf("transaction %d has no hash", index)
			}
			batch = append(batch, rpc.BatchElem{
				Method: "eth_getTransactionReceipt",
				Args:   []any{transaction.Hash},
				Result: &receipts[index],
			})
		}

		var batchErr error
		for attempt := 0; attempt < maxBatchTries; attempt++ {
			for index := range batch {
				batch[index].Error = nil
			}
			requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
			err := client.BatchCallContext(requestCtx, batch)
			cancel()
			if err != nil {
				batchErr = fmt.Errorf("batch eth_getTransactionReceipt: %w", err)
			} else {
				batchErr = nil
				for index, elem := range batch {
					if elem.Error != nil {
						batchErr = fmt.Errorf(
							"eth_getTransactionReceipt for transaction %d: %w",
							start+index,
							elem.Error,
						)
						break
					}
				}
			}
			if batchErr == nil {
				break
			}
			if !rateLimited(batchErr) || attempt == maxBatchTries-1 {
				return nil, batchErr
			}
			if err := waitForRPC(ctx, time.Duration(attempt+1)*retryBaseDelay); err != nil {
				return nil, err
			}
		}
	}
	return receipts, nil
}

func rateLimited(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "rate limit") ||
		strings.Contains(message, "too many requests") ||
		strings.Contains(message, "request limit")
}

func waitForRPC(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func methodUnsupported(err error) bool {
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) && (rpcErr.ErrorCode() == -32601 || rpcErr.ErrorCode() == -32004) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unsupported") || strings.Contains(message, "method not found")
}

func SubscriptionUnsupported(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "405") ||
		strings.Contains(message, "method not allowed") ||
		strings.Contains(message, "unsupported")
}

func (c *Client) SubscribeHeads(ctx context.Context) (<-chan Head, <-chan error, func(), error) {
	client, err := rpc.DialWebsocket(ctx, c.wssURL, "")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dial Base WSS: %w", err)
	}

	heads := make(chan Head, 16)
	sub, err := client.EthSubscribe(ctx, heads, "newHeads")
	if err != nil {
		client.Close()
		return nil, nil, nil, fmt.Errorf("subscribe newHeads: %w", err)
	}

	cancel := func() {
		sub.Unsubscribe()
		client.Close()
	}
	return heads, sub.Err(), cancel, nil
}

func BlockNumberArg(number uint64) string {
	return hexutil.EncodeBig(new(big.Int).SetUint64(number))
}
