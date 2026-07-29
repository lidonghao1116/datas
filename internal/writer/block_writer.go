package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/basewatch/base-analytics/internal/domain"
)

const maxRawBlockMessageBytes = 16 * 1024 * 1024

type RawBlockStore interface {
	Insert(ctx context.Context, envelope domain.RawBlockEnvelope) error
}

type BlockWriter struct {
	client *kgo.Client
	store  RawBlockStore
	logger *slog.Logger
}

func NewBlockWriter(
	brokers []string,
	topic, consumerGroup string,
	store RawBlockStore,
	logger *slog.Logger,
) (*BlockWriter, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxPartitionBytes(maxRawBlockMessageBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	return &BlockWriter{client: client, store: store, logger: logger}, nil
}

func (w *BlockWriter) Run(ctx context.Context) error {
	for {
		fetches := w.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fetchErr := range errs {
				w.logger.Error(
					"Kafka fetch failed",
					"topic", fetchErr.Topic,
					"partition", fetchErr.Partition,
					"error", fetchErr.Err,
				)
			}
			continue
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			if err := w.handle(ctx, record); err != nil {
				return err
			}
			if err := w.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit Kafka offset: %w", err)
			}
		}
	}
}

func (w *BlockWriter) handle(ctx context.Context, record *kgo.Record) error {
	var envelope domain.RawBlockEnvelope
	if err := json.Unmarshal(record.Value, &envelope); err != nil {
		return fmt.Errorf("decode record at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
	}
	if err := w.store.Insert(ctx, envelope); err != nil {
		return fmt.Errorf("persist block %d: %w", envelope.BlockNumber, err)
	}
	w.logger.Info(
		"block persisted",
		"block_number", envelope.BlockNumber,
		"block_hash", envelope.BlockHash,
		"kafka_partition", record.Partition,
		"kafka_offset", record.Offset,
	)
	return nil
}

func (w *BlockWriter) Close() {
	w.client.Close()
}
