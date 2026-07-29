package messaging

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/basewatch/base-analytics/internal/domain"
)

const maxRawBlockMessageBytes = 16 * 1024 * 1024

type RawBlockPublisher interface {
	Publish(ctx context.Context, envelope domain.RawBlockEnvelope) error
}

type KafkaRawBlockPublisher struct {
	client *kgo.Client
	topic  string
}

func NewKafkaRawBlockPublisher(brokers []string, topic string) (*KafkaRawBlockPublisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.ProducerBatchMaxBytes(maxRawBlockMessageBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	return &KafkaRawBlockPublisher{client: client, topic: topic}, nil
}

func (p *KafkaRawBlockPublisher) Publish(ctx context.Context, envelope domain.RawBlockEnvelope) error {
	value, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode raw block envelope: %w", err)
	}
	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(envelope.EventKey()),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "schema-version", Value: []byte(envelope.SchemaVersion)},
			{Key: "chain-id", Value: []byte(fmt.Sprintf("%d", envelope.ChainID))},
		},
	}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("publish block %d: %w", envelope.BlockNumber, err)
	}
	return nil
}

func (p *KafkaRawBlockPublisher) Close() {
	p.client.Close()
}
