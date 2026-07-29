package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clickhouseclient "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/basewatch/base-analytics/internal/domain"
)

type RawBlockStore struct {
	conn driver.Conn
}

type blockRecord struct {
	Number           hexutil.Uint64    `json:"number"`
	Hash             string            `json:"hash"`
	ParentHash       string            `json:"parentHash"`
	Timestamp        hexutil.Uint64    `json:"timestamp"`
	TransactionsRoot string            `json:"transactionsRoot"`
	ReceiptsRoot     string            `json:"receiptsRoot"`
	StateRoot        string            `json:"stateRoot"`
	GasUsed          hexutil.Uint64    `json:"gasUsed"`
	GasLimit         hexutil.Uint64    `json:"gasLimit"`
	Transactions     []json.RawMessage `json:"transactions"`
}

func OpenRawBlockStore(
	ctx context.Context,
	addr, database, username, password string,
) (*RawBlockStore, error) {
	conn, err := clickhouseclient.Open(&clickhouseclient.Options{
		Addr: []string{addr},
		Auth: clickhouseclient.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		DialTimeout: 10 * time.Second,
		Compression: &clickhouseclient.Compression{
			Method: clickhouseclient.CompressionZSTD,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping ClickHouse: %w", err)
	}
	return &RawBlockStore{conn: conn}, nil
}

func (s *RawBlockStore) Insert(ctx context.Context, envelope domain.RawBlockEnvelope) error {
	if err := envelope.Validate(); err != nil {
		return err
	}

	var block blockRecord
	if err := json.Unmarshal(envelope.Block, &block); err != nil {
		return fmt.Errorf("decode raw block %d: %w", envelope.BlockNumber, err)
	}

	if err := s.insertBlock(ctx, envelope, block); err != nil {
		return err
	}
	if err := s.insertTransactions(ctx, envelope, block.Transactions); err != nil {
		return err
	}
	if err := s.insertReceiptsAndLogs(ctx, envelope); err != nil {
		return err
	}
	return nil
}

func (s *RawBlockStore) insertBlock(
	ctx context.Context,
	envelope domain.RawBlockEnvelope,
	block blockRecord,
) error {
	err := s.conn.Exec(
		ctx,
		`INSERT INTO raw_blocks (
			chain_id, block_number, block_hash, parent_hash, block_time,
			transactions_root, receipts_root, state_root, gas_used, gas_limit,
			transaction_count, receipt_count, provider, observed_at,
			schema_version, is_canonical, raw_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		envelope.ChainID,
		envelope.BlockNumber,
		envelope.BlockHash,
		envelope.ParentHash,
		envelope.BlockTime,
		block.TransactionsRoot,
		block.ReceiptsRoot,
		block.StateRoot,
		uint64(block.GasUsed),
		uint64(block.GasLimit),
		uint32(len(block.Transactions)),
		uint32(len(envelope.Receipts)),
		envelope.Provider,
		envelope.ObservedAt,
		envelope.SchemaVersion,
		uint8(1),
		string(envelope.Block),
	)
	if err != nil {
		return fmt.Errorf("insert raw block %d: %w", envelope.BlockNumber, err)
	}
	return nil
}

func (s *RawBlockStore) insertTransactions(
	ctx context.Context,
	envelope domain.RawBlockEnvelope,
	transactions []json.RawMessage,
) error {
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO raw_transactions (
		chain_id, block_number, block_hash, block_time, transaction_hash,
		transaction_index, from_address, to_address, nonce, value_raw,
		gas, gas_price_raw, input, transaction_type, observed_at,
		is_canonical, raw_json
	)`)
	if err != nil {
		return fmt.Errorf("prepare transaction batch: %w", err)
	}
	for _, raw := range transactions {
		var item struct {
			Hash             string         `json:"hash"`
			TransactionIndex hexutil.Uint64 `json:"transactionIndex"`
			From             string         `json:"from"`
			To               *string        `json:"to"`
			Nonce            hexutil.Uint64 `json:"nonce"`
			Value            string         `json:"value"`
			Gas              hexutil.Uint64 `json:"gas"`
			GasPrice         string         `json:"gasPrice"`
			Input            string         `json:"input"`
			Type             hexutil.Uint64 `json:"type"`
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			return fmt.Errorf("decode transaction in block %d: %w", envelope.BlockNumber, err)
		}
		to := ""
		if item.To != nil {
			to = *item.To
		}
		if err := batch.Append(
			envelope.ChainID,
			envelope.BlockNumber,
			envelope.BlockHash,
			envelope.BlockTime,
			item.Hash,
			uint32(item.TransactionIndex),
			item.From,
			to,
			uint64(item.Nonce),
			item.Value,
			uint64(item.Gas),
			item.GasPrice,
			item.Input,
			uint8(item.Type),
			envelope.ObservedAt,
			uint8(1),
			string(raw),
		); err != nil {
			return fmt.Errorf("append transaction %s: %w", item.Hash, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send transaction batch for block %d: %w", envelope.BlockNumber, err)
	}
	return nil
}

func (s *RawBlockStore) insertReceiptsAndLogs(
	ctx context.Context,
	envelope domain.RawBlockEnvelope,
) error {
	receiptBatch, err := s.conn.PrepareBatch(ctx, `INSERT INTO raw_receipts (
		chain_id, block_number, block_hash, block_time, transaction_hash,
		transaction_index, status, gas_used, cumulative_gas_used,
		contract_address, log_count, observed_at, is_canonical, raw_json
	)`)
	if err != nil {
		return fmt.Errorf("prepare receipt batch: %w", err)
	}
	logBatch, err := s.conn.PrepareBatch(ctx, `INSERT INTO raw_logs (
		chain_id, block_number, block_hash, block_time, transaction_hash,
		transaction_index, log_index, address, topics, data, removed,
		observed_at, is_canonical, raw_json
	)`)
	if err != nil {
		return fmt.Errorf("prepare log batch: %w", err)
	}

	for _, raw := range envelope.Receipts {
		var receipt struct {
			TransactionHash   string            `json:"transactionHash"`
			TransactionIndex  hexutil.Uint64    `json:"transactionIndex"`
			Status            hexutil.Uint64    `json:"status"`
			GasUsed           hexutil.Uint64    `json:"gasUsed"`
			CumulativeGasUsed hexutil.Uint64    `json:"cumulativeGasUsed"`
			ContractAddress   *string           `json:"contractAddress"`
			Logs              []json.RawMessage `json:"logs"`
		}
		if err := json.Unmarshal(raw, &receipt); err != nil {
			return fmt.Errorf("decode receipt in block %d: %w", envelope.BlockNumber, err)
		}
		contractAddress := ""
		if receipt.ContractAddress != nil {
			contractAddress = *receipt.ContractAddress
		}
		if err := receiptBatch.Append(
			envelope.ChainID,
			envelope.BlockNumber,
			envelope.BlockHash,
			envelope.BlockTime,
			receipt.TransactionHash,
			uint32(receipt.TransactionIndex),
			uint8(receipt.Status),
			uint64(receipt.GasUsed),
			uint64(receipt.CumulativeGasUsed),
			contractAddress,
			uint32(len(receipt.Logs)),
			envelope.ObservedAt,
			uint8(1),
			string(raw),
		); err != nil {
			return fmt.Errorf("append receipt %s: %w", receipt.TransactionHash, err)
		}

		for _, rawLog := range receipt.Logs {
			var logItem struct {
				TransactionHash  string         `json:"transactionHash"`
				TransactionIndex hexutil.Uint64 `json:"transactionIndex"`
				LogIndex         hexutil.Uint64 `json:"logIndex"`
				Address          string         `json:"address"`
				Topics           []string       `json:"topics"`
				Data             string         `json:"data"`
				Removed          bool           `json:"removed"`
			}
			if err := json.Unmarshal(rawLog, &logItem); err != nil {
				return fmt.Errorf("decode log in transaction %s: %w", receipt.TransactionHash, err)
			}
			if err := logBatch.Append(
				envelope.ChainID,
				envelope.BlockNumber,
				envelope.BlockHash,
				envelope.BlockTime,
				logItem.TransactionHash,
				uint32(logItem.TransactionIndex),
				uint32(logItem.LogIndex),
				logItem.Address,
				logItem.Topics,
				logItem.Data,
				boolToUInt8(logItem.Removed),
				envelope.ObservedAt,
				uint8(1),
				string(rawLog),
			); err != nil {
				return fmt.Errorf("append log %d in transaction %s: %w", logItem.LogIndex, receipt.TransactionHash, err)
			}
		}
	}

	if err := receiptBatch.Send(); err != nil {
		return fmt.Errorf("send receipt batch for block %d: %w", envelope.BlockNumber, err)
	}
	if err := logBatch.Send(); err != nil {
		return fmt.Errorf("send log batch for block %d: %w", envelope.BlockNumber, err)
	}
	return nil
}

func (s *RawBlockStore) Close() error {
	return s.conn.Close()
}

func boolToUInt8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}
