package registry

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/basewatch/base-analytics/internal/domain"
)

type Resolver struct {
	store  Store
	reader ContractReader

	mu     sync.RWMutex
	pools  map[string]Pool
	tokens map[string]Token
}

func NewResolver(store Store, reader ContractReader) *Resolver {
	return &Resolver{
		store:  store,
		reader: reader,
		pools:  make(map[string]Pool),
		tokens: make(map[string]Token),
	}
}

func (r *Resolver) EnrichSwaps(
	ctx context.Context,
	swaps []domain.PoolSwap,
) []error {
	var errs []error
	resolved := make(map[string]resolvedPoolMetadata)
	budgetErrorReported := false
	for index := range swaps {
		swap := &swaps[index]
		key := registryKey(swap.ChainID, swap.PoolAddress)
		metadata, exists := resolved[key]
		if !exists && ctx.Err() != nil {
			swap.MetadataStatus = "unresolved"
			if !budgetErrorReported {
				errs = append(errs, fmt.Errorf("registry enrichment budget exhausted: %w", ctx.Err()))
				budgetErrorReported = true
			}
			continue
		}
		if !exists {
			pool, token0, token1, err := r.resolve(
				ctx,
				swap.ChainID,
				swap.PoolAddress,
				swap.ProtocolFamily,
				swap.BlockNumber,
				swap.ObservedAt,
			)
			metadata = resolvedPoolMetadata{
				pool:   pool,
				token0: token0,
				token1: token1,
				err:    err,
			}
			resolved[key] = metadata
			if err != nil {
				if ctx.Err() != nil {
					if !budgetErrorReported {
						errs = append(errs, fmt.Errorf(
							"registry enrichment budget exhausted: %w",
							ctx.Err(),
						))
						budgetErrorReported = true
					}
				} else {
					errs = append(errs, fmt.Errorf("resolve pool %s: %w", swap.PoolAddress, err))
				}
			}
		}
		if metadata.err != nil {
			swap.MetadataStatus = "unresolved"
			continue
		}
		metadata.apply(swap)
	}
	return errs
}

type resolvedPoolMetadata struct {
	pool   Pool
	token0 Token
	token1 Token
	err    error
}

func (m resolvedPoolMetadata) apply(swap *domain.PoolSwap) {
	swap.FactoryAddress = m.pool.FactoryAddress
	swap.Protocol = m.pool.Protocol
	swap.ProtocolVersion = m.pool.ProtocolVersion
	swap.ProtocolFamily = m.pool.ProtocolFamily
	swap.Token0Address = m.pool.Token0Address
	swap.Token1Address = m.pool.Token1Address
	swap.Token0Symbol = m.token0.Symbol
	swap.Token1Symbol = m.token1.Symbol
	swap.Token0Decimals = m.token0.Decimals
	swap.Token1Decimals = m.token1.Decimals
	swap.MetadataStatus = metadataStatus(m.token0, m.token1)
}

func (r *Resolver) resolve(
	ctx context.Context,
	chainID uint64,
	poolAddress, protocolFamily string,
	blockNumber uint64,
	observedAt time.Time,
) (Pool, Token, Token, error) {
	poolAddress, err := normalizedAddress(poolAddress)
	if err != nil {
		return Pool{}, Token{}, Token{}, err
	}
	pool, exists, err := r.getPool(ctx, chainID, poolAddress)
	if err != nil {
		return Pool{}, Token{}, Token{}, err
	}
	if !exists {
		pool, err = r.discoverPool(
			ctx,
			chainID,
			poolAddress,
			protocolFamily,
			blockNumber,
			observedAt,
		)
		if err != nil {
			return Pool{}, Token{}, Token{}, err
		}
	}

	token0, err := r.resolveToken(ctx, chainID, pool.Token0Address, observedAt)
	if err != nil {
		return Pool{}, Token{}, Token{}, err
	}
	token1, err := r.resolveToken(ctx, chainID, pool.Token1Address, observedAt)
	if err != nil {
		return Pool{}, Token{}, Token{}, err
	}
	return pool, token0, token1, nil
}

func (r *Resolver) getPool(
	ctx context.Context,
	chainID uint64,
	address string,
) (Pool, bool, error) {
	key := registryKey(chainID, address)
	r.mu.RLock()
	pool, exists := r.pools[key]
	r.mu.RUnlock()
	if exists {
		return pool, true, nil
	}
	pool, exists, err := r.store.GetPool(ctx, chainID, address)
	if err != nil || !exists {
		return pool, exists, err
	}
	pool, err = r.classifyStoredPool(ctx, pool)
	if err != nil {
		return Pool{}, false, err
	}
	r.mu.Lock()
	r.pools[key] = pool
	r.mu.Unlock()
	return pool, true, nil
}

func (r *Resolver) classifyStoredPool(ctx context.Context, pool Pool) (Pool, error) {
	changed := false
	if pool.FactoryAddress == "" {
		if factoryAddress, err := r.reader.Address(
			ctx,
			pool.Address,
			selectorFactory,
		); err == nil {
			pool.FactoryAddress = factoryAddress
			changed = true
		}
	}
	if pool.FactoryAddress != "" {
		factory, exists, err := r.store.GetFactory(
			ctx,
			pool.ChainID,
			pool.FactoryAddress,
		)
		if err != nil {
			return Pool{}, err
		}
		if exists && (pool.Protocol != factory.Protocol ||
			pool.ProtocolVersion != factory.ProtocolVersion ||
			pool.ProtocolFamily != factory.ProtocolFamily) {
			pool.Protocol = factory.Protocol
			pool.ProtocolVersion = factory.ProtocolVersion
			pool.ProtocolFamily = factory.ProtocolFamily
			changed = true
		}
	}
	if changed {
		if err := r.store.UpsertPool(ctx, pool); err != nil {
			return Pool{}, err
		}
	}
	return pool, nil
}

func (r *Resolver) discoverPool(
	ctx context.Context,
	chainID uint64,
	address, protocolFamily string,
	blockNumber uint64,
	observedAt time.Time,
) (Pool, error) {
	var token0, token1, factoryAddress string
	if batchReader, ok := r.reader.(BatchMetadataReader); ok {
		var factoryKnown bool
		var err error
		token0, token1, factoryAddress, factoryKnown, err = batchReader.PoolMetadata(ctx, address)
		if err != nil {
			return Pool{}, err
		}
		if !factoryKnown {
			factoryAddress = ""
		}
	} else {
		var err error
		token0, err = r.reader.Address(ctx, address, selectorToken0)
		if err != nil {
			return Pool{}, fmt.Errorf("read token0: %w", err)
		}
		token1, err = r.reader.Address(ctx, address, selectorToken1)
		if err != nil {
			return Pool{}, fmt.Errorf("read token1: %w", err)
		}
		factoryAddress, _ = r.reader.Address(ctx, address, selectorFactory)
	}

	pool := Pool{
		ChainID:         chainID,
		Address:         address,
		FactoryAddress:  factoryAddress,
		Protocol:        "unknown",
		ProtocolFamily:  protocolFamily,
		Token0Address:   token0,
		Token1Address:   token1,
		DiscoveredBlock: blockNumber,
		ObservedAt:      observedAt,
	}
	if factoryAddress != "" {
		factory, exists, err := r.store.GetFactory(ctx, chainID, factoryAddress)
		if err != nil {
			return Pool{}, err
		}
		if exists {
			pool.Protocol = factory.Protocol
			pool.ProtocolVersion = factory.ProtocolVersion
			pool.ProtocolFamily = factory.ProtocolFamily
		}
	}
	if err := r.store.UpsertPool(ctx, pool); err != nil {
		return Pool{}, err
	}
	r.mu.Lock()
	r.pools[registryKey(chainID, address)] = pool
	r.mu.Unlock()
	return pool, nil
}

func (r *Resolver) resolveToken(
	ctx context.Context,
	chainID uint64,
	address string,
	observedAt time.Time,
) (Token, error) {
	key := registryKey(chainID, address)
	r.mu.RLock()
	token, exists := r.tokens[key]
	r.mu.RUnlock()
	if exists && token.DecimalsKnown && token.SymbolKnown {
		return token, nil
	}
	token, exists, err := r.store.GetToken(ctx, chainID, address)
	if err != nil {
		return Token{}, err
	}
	if !exists {
		token = Token{ChainID: chainID, Address: address, ObservedAt: observedAt}
	}

	if batchReader, ok := r.reader.(BatchMetadataReader); ok &&
		(!token.DecimalsKnown || !token.SymbolKnown) {
		decimals, decimalsKnown, symbol, symbolKnown := batchReader.TokenMetadata(ctx, address)
		if !token.DecimalsKnown && decimalsKnown {
			token.Decimals = decimals
			token.DecimalsKnown = true
		}
		if !token.SymbolKnown && symbolKnown {
			token.Symbol = symbol
			token.SymbolKnown = true
		}
	} else {
		if !token.DecimalsKnown {
			if decimals, err := r.reader.Uint8(ctx, address, selectorDecimals); err == nil {
				token.Decimals = decimals
				token.DecimalsKnown = true
			}
		}
		if !token.SymbolKnown {
			if symbol, err := r.reader.String(ctx, address, selectorSymbol); err == nil {
				token.Symbol = symbol
				token.SymbolKnown = true
			}
		}
	}
	if err := r.store.UpsertToken(ctx, token); err != nil {
		return Token{}, err
	}
	if token.DecimalsKnown && token.SymbolKnown {
		r.mu.Lock()
		r.tokens[key] = token
		r.mu.Unlock()
	}
	return token, nil
}

func metadataStatus(token0, token1 Token) string {
	if token0.DecimalsKnown && token1.DecimalsKnown &&
		token0.SymbolKnown && token1.SymbolKnown {
		return "resolved"
	}
	return "partial"
}

func registryKey(chainID uint64, address string) string {
	return fmt.Sprintf("%d:%s", chainID, strings.ToLower(address))
}

func normalizedAddress(address string) (string, error) {
	if !common.IsHexAddress(address) {
		return "", fmt.Errorf("invalid address %q", address)
	}
	return strings.ToLower(common.HexToAddress(address).Hex()), nil
}
