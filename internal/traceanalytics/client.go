package traceanalytics

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/rpc"
)

type RPCClient struct {
	url           string
	tracerTimeout string
}

func NewRPCClient(url, tracerTimeout string) (*RPCClient, error) {
	if url == "" || tracerTimeout == "" {
		return nil, fmt.Errorf("archive RPC URL and tracer timeout are required")
	}
	return &RPCClient{url: url, tracerTimeout: tracerTimeout}, nil
}

func (c *RPCClient) TraceTransaction(
	ctx context.Context,
	transactionHash string,
) (CallFrame, json.RawMessage, error) {
	client, err := rpc.DialContext(ctx, c.url)
	if err != nil {
		return CallFrame{}, nil, fmt.Errorf("dial archive RPC: %w", err)
	}
	defer client.Close()

	var raw json.RawMessage
	options := map[string]any{
		"tracer":  "callTracer",
		"timeout": c.tracerTimeout,
		"tracerConfig": map[string]any{
			"onlyTopCall": false,
			"withLog":     true,
		},
	}
	if err := client.CallContext(
		ctx,
		&raw,
		"debug_traceTransaction",
		transactionHash,
		options,
	); err != nil {
		return CallFrame{}, nil, fmt.Errorf(
			"debug_traceTransaction %s: %w",
			transactionHash,
			err,
		)
	}
	if len(raw) == 0 || string(raw) == "null" {
		return CallFrame{}, nil, fmt.Errorf("trace %s returned no result", transactionHash)
	}
	var root CallFrame
	if err := json.Unmarshal(raw, &root); err != nil {
		return CallFrame{}, nil, fmt.Errorf("decode trace %s: %w", transactionHash, err)
	}
	return root, raw, nil
}
