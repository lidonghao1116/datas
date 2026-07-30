package flashblocks

import (
	"context"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/rpc"
)

type Client struct {
	preconfHTTPURL   string
	canonicalHTTPURL string
	wssURL           string
}

func NewClient(preconfHTTPURL, canonicalHTTPURL, wssURL string) *Client {
	return &Client{
		preconfHTTPURL:   preconfHTTPURL,
		canonicalHTTPURL: canonicalHTTPURL,
		wssURL:           wssURL,
	}
}

func (c *Client) SubscribePendingLogs(
	ctx context.Context,
	topics []string,
) (<-chan PendingLog, <-chan error, func(), error) {
	client, err := rpc.DialWebsocket(ctx, c.wssURL, "")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dial Flashblocks WSS: %w", err)
	}
	logs := make(chan PendingLog, 4096)
	filter := map[string]any{
		"topics": []any{topics},
	}
	subscription, err := client.EthSubscribe(ctx, logs, "pendingLogs", filter)
	if err != nil {
		client.Close()
		return nil, nil, nil, fmt.Errorf("subscribe pendingLogs: %w", err)
	}
	closeSubscription := func() {
		subscription.Unsubscribe()
		client.Close()
	}
	return logs, subscription.Err(), closeSubscription, nil
}

func (c *Client) PendingLogs(
	ctx context.Context,
	topics []string,
) ([]PendingLog, error) {
	var logs []PendingLog
	filter := map[string]any{
		"fromBlock": "pending",
		"toBlock":   "pending",
		"topics":    []any{topics},
	}
	if err := c.call(
		ctx,
		c.preconfHTTPURL,
		&logs,
		"eth_getLogs",
		filter,
	); err != nil {
		return nil, err
	}
	return logs, nil
}

func (c *Client) PendingTransactions(
	ctx context.Context,
) (map[string]Transaction, error) {
	var block *struct {
		Transactions []Transaction `json:"transactions"`
	}
	if err := c.call(
		ctx,
		c.preconfHTTPURL,
		&block,
		"eth_getBlockByNumber",
		"pending",
		true,
	); err != nil {
		return nil, err
	}
	result := make(map[string]Transaction)
	if block == nil {
		return result, nil
	}
	for _, transaction := range block.Transactions {
		result[strings.ToLower(transaction.Hash)] = transaction
	}
	return result, nil
}

func (c *Client) PendingSnapshot(
	ctx context.Context,
	topics []string,
) ([]PendingLog, map[string]Transaction, error) {
	client, err := rpc.DialContext(ctx, c.preconfHTTPURL)
	if err != nil {
		return nil, nil, fmt.Errorf("dial Flashblocks HTTP: %w", err)
	}
	defer client.Close()
	var logs []PendingLog
	var block *struct {
		Transactions []Transaction `json:"transactions"`
	}
	filter := map[string]any{
		"fromBlock": "pending",
		"toBlock":   "pending",
		"topics":    []any{topics},
	}
	batch := []rpc.BatchElem{
		{
			Method: "eth_getLogs",
			Args:   []any{filter},
			Result: &logs,
		},
		{
			Method: "eth_getBlockByNumber",
			Args:   []any{"pending", true},
			Result: &block,
		},
	}
	if err := client.BatchCallContext(ctx, batch); err != nil {
		return nil, nil, fmt.Errorf("fetch pending Flashblocks snapshot: %w", err)
	}
	for _, element := range batch {
		if element.Error != nil {
			return nil, nil, fmt.Errorf("%s: %w", element.Method, element.Error)
		}
	}
	transactions := make(map[string]Transaction)
	if block != nil {
		for _, transaction := range block.Transactions {
			transactions[strings.ToLower(transaction.Hash)] = transaction
		}
	}
	return logs, transactions, nil
}

func (c *Client) TransactionByHash(
	ctx context.Context,
	hash string,
) (Transaction, bool, error) {
	var transaction *Transaction
	if err := c.call(ctx, c.preconfHTTPURL, &transaction, "eth_getTransactionByHash", hash); err != nil {
		return Transaction{}, false, err
	}
	if transaction == nil {
		return Transaction{}, false, nil
	}
	return *transaction, true, nil
}

func (c *Client) ReceiptByHash(
	ctx context.Context,
	hash string,
) (Receipt, bool, error) {
	var receipt *Receipt
	if err := c.call(ctx, c.canonicalHTTPURL, &receipt, "eth_getTransactionReceipt", hash); err != nil {
		return Receipt{}, false, err
	}
	if receipt == nil {
		return Receipt{}, false, nil
	}
	return *receipt, true, nil
}

func (c *Client) call(
	ctx context.Context,
	url string,
	result any,
	method string,
	args ...any,
) error {
	client, err := rpc.DialContext(ctx, url)
	if err != nil {
		return fmt.Errorf("dial Flashblocks HTTP: %w", err)
	}
	defer client.Close()
	if err := client.CallContext(ctx, result, method, args...); err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	return nil
}
