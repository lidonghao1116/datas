package registry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/basewatch/base-analytics/internal/domain"
)

func TestResolverEnrichesSwapFromVerifiedFactory(t *testing.T) {
	const (
		poolAddress    = "0x1111111111111111111111111111111111111111"
		factoryAddress = "0x420dd381b31aef6683db6b902084cb0ffec40da"
		token0Address  = "0x2222222222222222222222222222222222222222"
		token1Address  = "0x3333333333333333333333333333333333333333"
	)
	store := newFakeStore()
	store.factories[registryKey(8453, factoryAddress)] = Factory{
		ChainID:         8453,
		Address:         factoryAddress,
		Protocol:        "aerodrome",
		ProtocolVersion: "classic",
		ProtocolFamily:  "uniswap_v2_compatible",
		Verified:        true,
	}
	reader := &fakeReader{
		addresses: map[string]string{
			poolAddress + ":" + selectorToken0:  token0Address,
			poolAddress + ":" + selectorToken1:  token1Address,
			poolAddress + ":" + selectorFactory: factoryAddress,
		},
		decimals: map[string]uint8{
			token0Address: 6,
			token1Address: 18,
		},
		symbols: map[string]string{
			token0Address: "USDC",
			token1Address: "WETH",
		},
	}
	resolver := NewResolver(store, reader)
	swaps := []domain.PoolSwap{
		{
			EventMeta: domain.EventMeta{
				ChainID:     8453,
				BlockNumber: 100,
				ObservedAt:  time.Now(),
			},
			PoolAddress:    poolAddress,
			ProtocolFamily: "uniswap_v2_compatible",
		},
		{
			EventMeta: domain.EventMeta{
				ChainID:     8453,
				BlockNumber: 100,
				ObservedAt:  time.Now(),
			},
			PoolAddress:    poolAddress,
			ProtocolFamily: "uniswap_v2_compatible",
		},
	}

	if errs := resolver.EnrichSwaps(context.Background(), swaps); len(errs) != 0 {
		t.Fatalf("unexpected enrichment errors: %v", errs)
	}
	for _, swap := range swaps {
		if swap.Protocol != "aerodrome" || swap.ProtocolVersion != "classic" {
			t.Fatalf("unexpected protocol %s/%s", swap.Protocol, swap.ProtocolVersion)
		}
		if swap.Token0Address != token0Address || swap.Token1Address != token1Address {
			t.Fatalf("unexpected tokens %s/%s", swap.Token0Address, swap.Token1Address)
		}
		if swap.Token0Symbol != "USDC" || swap.Token1Symbol != "WETH" {
			t.Fatalf("unexpected symbols %s/%s", swap.Token0Symbol, swap.Token1Symbol)
		}
		if swap.Token0Decimals != 6 || swap.Token1Decimals != 18 {
			t.Fatalf("unexpected decimals %d/%d", swap.Token0Decimals, swap.Token1Decimals)
		}
		if swap.MetadataStatus != "resolved" {
			t.Fatalf("expected resolved metadata, got %s", swap.MetadataStatus)
		}
	}
	if reader.addressCalls != 3 {
		t.Fatalf("expected one lookup per pool in a block, got %d address calls", reader.addressCalls)
	}

	second := []domain.PoolSwap{{
		EventMeta:      swaps[0].EventMeta,
		PoolAddress:    poolAddress,
		ProtocolFamily: "uniswap_v2_compatible",
	}}
	if errs := resolver.EnrichSwaps(context.Background(), second); len(errs) != 0 {
		t.Fatalf("unexpected cached enrichment errors: %v", errs)
	}
	if reader.addressCalls != 3 {
		t.Fatalf("expected pool metadata to be cached, got %d address calls", reader.addressCalls)
	}
}

func TestDecodeContractString(t *testing.T) {
	dynamic := make([]byte, 96)
	dynamic[31] = 32
	dynamic[63] = 4
	copy(dynamic[64:], []byte("USDC"))
	value, err := decodeContractString(dynamic)
	if err != nil {
		t.Fatal(err)
	}
	if value != "USDC" {
		t.Fatalf("expected USDC, got %q", value)
	}

	bytes32 := make([]byte, 32)
	copy(bytes32, []byte("WETH"))
	value, err = decodeContractString(bytes32)
	if err != nil {
		t.Fatal(err)
	}
	if value != "WETH" {
		t.Fatalf("expected WETH, got %q", value)
	}
}

func TestRateLimitDetection(t *testing.T) {
	if isRateLimited(nil) {
		t.Fatal("nil error must not be treated as rate limiting")
	}
	for _, message := range []string{
		"429 Too Many Requests",
		"over rate limit",
		"request rate limited",
	} {
		if !isRateLimited(fmt.Errorf("%s", message)) {
			t.Fatalf("expected %q to be detected as rate limiting", message)
		}
	}
	if isRateLimited(fmt.Errorf("execution reverted")) {
		t.Fatal("execution revert must not be treated as rate limiting")
	}
}

func TestResolverStopsRPCWorkWhenEnrichmentBudgetExpires(t *testing.T) {
	reader := &fakeReader{}
	resolver := NewResolver(newFakeStore(), reader)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	swaps := []domain.PoolSwap{
		{
			EventMeta:   domain.EventMeta{ChainID: 8453},
			PoolAddress: "0x1111111111111111111111111111111111111111",
		},
		{
			EventMeta:   domain.EventMeta{ChainID: 8453},
			PoolAddress: "0x2222222222222222222222222222222222222222",
		},
	}

	errs := resolver.EnrichSwaps(ctx, swaps)
	if len(errs) != 1 {
		t.Fatalf("expected one budget error, got %v", errs)
	}
	if reader.addressCalls != 0 {
		t.Fatalf("expected no RPC calls after budget expiry, got %d", reader.addressCalls)
	}
	for _, swap := range swaps {
		if swap.MetadataStatus != "unresolved" {
			t.Fatalf("expected unresolved metadata, got %s", swap.MetadataStatus)
		}
	}
}

type fakeStore struct {
	factories map[string]Factory
	pools     map[string]Pool
	tokens    map[string]Token
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		factories: make(map[string]Factory),
		pools:     make(map[string]Pool),
		tokens:    make(map[string]Token),
	}
}

func (s *fakeStore) GetFactory(
	_ context.Context,
	chainID uint64,
	address string,
) (Factory, bool, error) {
	value, exists := s.factories[registryKey(chainID, address)]
	return value, exists, nil
}

func (s *fakeStore) GetPool(
	_ context.Context,
	chainID uint64,
	address string,
) (Pool, bool, error) {
	value, exists := s.pools[registryKey(chainID, address)]
	return value, exists, nil
}

func (s *fakeStore) UpsertPool(_ context.Context, pool Pool) error {
	s.pools[registryKey(pool.ChainID, pool.Address)] = pool
	return nil
}

func (s *fakeStore) GetToken(
	_ context.Context,
	chainID uint64,
	address string,
) (Token, bool, error) {
	value, exists := s.tokens[registryKey(chainID, address)]
	return value, exists, nil
}

func (s *fakeStore) UpsertToken(_ context.Context, token Token) error {
	s.tokens[registryKey(token.ChainID, token.Address)] = token
	return nil
}

type fakeReader struct {
	addresses    map[string]string
	decimals     map[string]uint8
	symbols      map[string]string
	addressCalls int
}

func (r *fakeReader) Address(
	_ context.Context,
	contract, selector string,
) (string, error) {
	r.addressCalls++
	value, exists := r.addresses[contract+":"+selector]
	if !exists {
		return "", fmt.Errorf("missing address response")
	}
	return value, nil
}

func (r *fakeReader) Uint8(
	_ context.Context,
	contract, _ string,
) (uint8, error) {
	value, exists := r.decimals[contract]
	if !exists {
		return 0, fmt.Errorf("missing decimals response")
	}
	return value, nil
}

func (r *fakeReader) String(
	_ context.Context,
	contract, _ string,
) (string, error) {
	value, exists := r.symbols[contract]
	if !exists {
		return "", fmt.Errorf("missing symbol response")
	}
	return value, nil
}
