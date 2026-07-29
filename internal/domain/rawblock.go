package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

const RawBlockSchemaVersion = "1"

type RawBlockEnvelope struct {
	SchemaVersion string            `json:"schema_version"`
	ChainID       uint64            `json:"chain_id"`
	BlockNumber   uint64            `json:"block_number"`
	BlockHash     string            `json:"block_hash"`
	ParentHash    string            `json:"parent_hash"`
	BlockTime     time.Time         `json:"block_time"`
	ObservedAt    time.Time         `json:"observed_at"`
	Provider      string            `json:"provider"`
	Block         json.RawMessage   `json:"block"`
	Receipts      []json.RawMessage `json:"receipts"`
}

func (e RawBlockEnvelope) Validate() error {
	if e.SchemaVersion != RawBlockSchemaVersion {
		return fmt.Errorf("unsupported schema version %q", e.SchemaVersion)
	}
	if e.ChainID == 0 {
		return fmt.Errorf("chain_id is required")
	}
	if e.BlockHash == "" || e.ParentHash == "" {
		return fmt.Errorf("block and parent hashes are required")
	}
	if len(e.Block) == 0 {
		return fmt.Errorf("raw block is required")
	}
	return nil
}

func (e RawBlockEnvelope) EventKey() string {
	return fmt.Sprintf("%d:%s", e.ChainID, e.BlockHash)
}
