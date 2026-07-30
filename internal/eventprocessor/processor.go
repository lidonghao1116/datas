package eventprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/basewatch/base-analytics/internal/domain"
	"github.com/basewatch/base-analytics/internal/parser/logs"
)

const maxRawBlockMessageBytes = 16 * 1024 * 1024

type EventStore interface {
	Insert(
		ctx context.Context,
		result logs.Result,
		reorganization *domain.ChainReorganization,
	) error
}

type SwapEnricher interface {
	EnrichSwaps(ctx context.Context, swaps []domain.PoolSwap) []error
}

type Processor struct {
	client            *kgo.Client
	parser            *logs.Parser
	enricher          SwapEnricher
	store             EventStore
	logger            *slog.Logger
	enrichmentTimeout time.Duration
}

func New(
	brokers []string,
	topic, consumerGroup string,
	parser *logs.Parser,
	enricher SwapEnricher,
	store EventStore,
	logger *slog.Logger,
	enrichmentTimeout time.Duration,
) (*Processor, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxPartitionBytes(maxRawBlockMessageBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("create event parser Kafka consumer: %w", err)
	}
	return &Processor{
		client:            client,
		parser:            parser,
		enricher:          enricher,
		store:             store,
		logger:            logger,
		enrichmentTimeout: enrichmentTimeout,
	}, nil
}

func (p *Processor) Run(ctx context.Context) error {
	for {
		fetches := p.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fetchErr := range errs {
				p.logger.Error(
					"event parser Kafka fetch failed",
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
			if err := p.handle(ctx, record); err != nil {
				return err
			}
			if err := p.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit event parser Kafka offset: %w", err)
			}
		}
	}
}

func (p *Processor) handle(ctx context.Context, record *kgo.Record) error {
	var envelope domain.RawBlockEnvelope
	if err := json.Unmarshal(record.Value, &envelope); err != nil {
		return fmt.Errorf(
			"decode event parser record at %s/%d/%d: %w",
			record.Topic,
			record.Partition,
			record.Offset,
			err,
		)
	}
	result, err := p.parser.Parse(envelope)
	if err != nil {
		return fmt.Errorf("parse events from block %d: %w", envelope.BlockNumber, err)
	}
	if p.enricher != nil {
		enrichmentCtx := ctx
		cancel := func() {}
		if p.enrichmentTimeout > 0 {
			enrichmentCtx, cancel = context.WithTimeout(ctx, p.enrichmentTimeout)
		}
		enrichmentErrors := p.enricher.EnrichSwaps(enrichmentCtx, result.Swaps)
		cancel()
		for _, enrichErr := range enrichmentErrors {
			p.logger.Warn(
				"swap metadata enrichment failed",
				"block_number", envelope.BlockNumber,
				"error", enrichErr,
			)
		}
	}
	if err := p.store.Insert(ctx, result, envelope.Reorganization); err != nil {
		return fmt.Errorf("persist events from block %d: %w", envelope.BlockNumber, err)
	}
	p.logger.Info(
		"block events parsed",
		"block_number", envelope.BlockNumber,
		"transfer_count", len(result.Transfers),
		"swap_count", len(result.Swaps),
		"kafka_partition", record.Partition,
		"kafka_offset", record.Offset,
	)
	return nil
}

func (p *Processor) Close() {
	p.client.Close()
}
