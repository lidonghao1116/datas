package flashblocks

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/basewatch/base-analytics/internal/alerting"
	"github.com/basewatch/base-analytics/internal/domain"
	parserlogs "github.com/basewatch/base-analytics/internal/parser/logs"
	"github.com/basewatch/base-analytics/internal/valuation"
)

func TestWorkerCreatesPreconfirmedAlert(t *testing.T) {
	now := time.Now().UTC()
	source := &fakeSource{
		transaction: Transaction{
			Hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			From: "0x1111111111111111111111111111111111111111",
		},
	}
	enrichment := fakeEnrichmentStore{enrichment: Enrichment{
		Valuation: valuation.Candidate{
			Price0: valuation.Price{
				Raw:       "2",
				Source:    "ave",
				FetchedAt: now,
			},
			Price1: valuation.Price{
				Raw:       "1",
				Source:    "ave",
				FetchedAt: now,
			},
		},
	}}
	state := &fakeStateStore{}
	builder := &fakeAlertBuilder{}
	calculator, err := valuation.NewCalculator(time.Hour, "100")
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(
		source,
		enrichment,
		state,
		builder,
		parserlogs.NewParser(),
		calculator,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		WorkerConfig{
			ChainID:              8453,
			ScoreVersion:         "smart-v1",
			ReconciliationTTL:    30 * time.Second,
			ReconciliationBatch:  10,
			ReconciliationPoll:   time.Second,
			ReconnectDelay:       time.Second,
			RequestTimeout:       time.Second,
			FallbackPollInterval: 200 * time.Millisecond,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	pendingLog := PendingLog{
		Address:          "0x2222222222222222222222222222222222222222",
		Topics:           []string{parserlogs.V2SwapDecoder{}.Topic0(), indexed("0x3333333333333333333333333333333333333333"), indexed("0x4444444444444444444444444444444444444444")},
		Data:             words(0, 200, 100, 0),
		BlockHash:        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		BlockNumber:      "0x10",
		BlockTimestamp:   fmt.Sprintf("0x%x", now.Unix()),
		TransactionHash:  source.transaction.Hash,
		TransactionIndex: "0x1",
		LogIndex:         "0x2",
	}
	if err := worker.handlePendingLog(context.Background(), pendingLog); err != nil {
		t.Fatal(err)
	}
	if !state.inserted || len(state.alerts) != 1 {
		t.Fatalf("expected persisted preconfirmation alert, got %+v", state)
	}
	if state.alerts[0].Type != "preconfirmed_large_buy" {
		t.Fatalf("unexpected alert type %s", state.alerts[0].Type)
	}
	if builder.candidate.WalletAddress != strings.ToLower(source.transaction.From) {
		t.Fatalf("unexpected wallet %s", builder.candidate.WalletAddress)
	}
}

func TestReceiptContainsCanonicalLog(t *testing.T) {
	item := Reconciliation{Preconfirmation: Preconfirmation{
		LogIndex:    7,
		PoolAddress: "0x2222222222222222222222222222222222222222",
	}}
	receipt := Receipt{Logs: []PendingLog{{
		Address:  strings.ToUpper(item.PoolAddress),
		LogIndex: "0x7",
	}}}
	if !receiptContains(receipt, item) {
		t.Fatal("expected receipt log match")
	}
	receipt.Logs[0].Removed = true
	if receiptContains(receipt, item) {
		t.Fatal("removed log must not confirm a preconfirmation")
	}
}

func indexed(address string) string {
	return "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(address, "0x")
}

func words(values ...uint64) string {
	var builder strings.Builder
	builder.WriteString("0x")
	for _, value := range values {
		fmt.Fprintf(&builder, "%064x", value)
	}
	return builder.String()
}

type fakeSource struct {
	transaction Transaction
}

func (f *fakeSource) SubscribePendingLogs(
	context.Context,
	[]string,
) (<-chan PendingLog, <-chan error, func(), error) {
	return make(chan PendingLog), make(chan error), func() {}, nil
}

func (f *fakeSource) TransactionByHash(
	context.Context,
	string,
) (Transaction, bool, error) {
	return f.transaction, true, nil
}

func (f *fakeSource) PendingSnapshot(
	context.Context,
	[]string,
) ([]PendingLog, map[string]Transaction, error) {
	return nil, map[string]Transaction{
		strings.ToLower(f.transaction.Hash): f.transaction,
	}, nil
}

func (f *fakeSource) ReceiptByHash(
	context.Context,
	string,
) (Receipt, bool, error) {
	return Receipt{}, false, nil
}

type fakeEnrichmentStore struct {
	enrichment Enrichment
}

func (f fakeEnrichmentStore) EnrichPendingSwap(
	_ context.Context,
	swap domain.PoolSwap,
	_ string,
	_ string,
) (Enrichment, bool, error) {
	f.enrichment.Valuation.Swap = swap
	f.enrichment.Valuation.Swap.Token0Address = "0x5555555555555555555555555555555555555555"
	f.enrichment.Valuation.Swap.Token1Address = "0x6666666666666666666666666666666666666666"
	f.enrichment.Valuation.Swap.Token0Symbol = "MEME"
	f.enrichment.Valuation.Swap.Token1Symbol = "USDC"
	f.enrichment.Valuation.Swap.Token0Decimals = 0
	f.enrichment.Valuation.Swap.Token1Decimals = 0
	return f.enrichment, true, nil
}

type fakeAlertBuilder struct {
	candidate alerting.Candidate
}

func (f *fakeAlertBuilder) BuildAlerts(
	candidate alerting.Candidate,
) ([]alerting.Alert, error) {
	f.candidate = candidate
	return []alerting.Alert{{
		Key:             "large",
		Type:            "large_buy",
		Severity:        "high",
		ChainID:         candidate.ChainID,
		BlockNumber:     candidate.BlockNumber,
		TransactionHash: candidate.TransactionHash,
		TokenAddress:    candidate.BoughtTokenAddress,
		TokenSymbol:     candidate.BoughtTokenSymbol,
		Title:           "large buy",
		Payload:         []byte(`{"test":true}`),
	}}, nil
}

type fakeStateStore struct {
	inserted bool
	alerts   []alerting.Alert
}

func (f *fakeStateStore) InsertPending(
	_ context.Context,
	_ Preconfirmation,
	alerts []alerting.Alert,
) (bool, error) {
	f.inserted = true
	f.alerts = alerts
	return true, nil
}

func (*fakeStateStore) PendingByKey(context.Context, string) (Reconciliation, bool, error) {
	return Reconciliation{}, false, nil
}

func (*fakeStateStore) PendingReconciliations(context.Context, int) ([]Reconciliation, error) {
	return nil, nil
}

func (*fakeStateStore) DeferReconciliation(context.Context, string, time.Time, string) error {
	return nil
}

func (*fakeStateStore) Resolve(
	context.Context,
	Reconciliation,
	string,
	time.Time,
	uint64,
	string,
	alerting.Alert,
) error {
	return nil
}
