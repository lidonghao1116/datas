package clickhouse

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/basewatch/base-analytics/internal/domain"
)

var rawCanonicalTables = []string{
	"raw_blocks",
	"raw_transactions",
	"raw_receipts",
	"raw_logs",
}

var eventCanonicalTables = []string{
	"erc20_transfers",
	"dex_pool_swaps",
}

func applyCanonicalCorrection(
	ctx context.Context,
	conn driver.Conn,
	chainID uint64,
	reorganization *domain.ChainReorganization,
	tables []string,
) error {
	if reorganization == nil {
		return nil
	}
	hashes := orphanedHashes(reorganization)
	if len(hashes) == 0 {
		return fmt.Errorf("canonical correction has no orphaned block hashes")
	}
	for _, table := range tables {
		query := fmt.Sprintf(
			`ALTER TABLE %s UPDATE is_canonical = 0
			 WHERE chain_id = ? AND has(?, block_hash)
			 SETTINGS mutations_sync = 2`,
			table,
		)
		if err := conn.Exec(ctx, query, chainID, hashes); err != nil {
			return fmt.Errorf("mark orphaned rows in %s: %w", table, err)
		}
	}
	return nil
}

func orphanedHashes(reorganization *domain.ChainReorganization) []string {
	if reorganization == nil {
		return nil
	}
	hashes := make([]string, 0, len(reorganization.OrphanedBlocks))
	seen := make(map[string]struct{}, len(reorganization.OrphanedBlocks))
	for _, block := range reorganization.OrphanedBlocks {
		if block.BlockHash == "" {
			continue
		}
		if _, exists := seen[block.BlockHash]; exists {
			continue
		}
		seen[block.BlockHash] = struct{}{}
		hashes = append(hashes, block.BlockHash)
	}
	return hashes
}
