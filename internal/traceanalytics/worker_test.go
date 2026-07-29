package traceanalytics

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestWorkerStoresSuccessfulTrace(t *testing.T) {
	store := &fakeTraceStore{candidates: []Candidate{testCandidate()}}
	client := &fakeTraceClient{root: CallFrame{
		Type: "CALL",
		From: "0x1111111111111111111111111111111111111111",
		To:   "0x2222222222222222222222222222222222222222",
	}}
	worker := newTestWorker(t, store, client)
	processed, err := worker.processBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 || len(store.results) != 1 {
		t.Fatalf("expected one stored result, got processed=%d results=%d", processed, len(store.results))
	}
	if len(store.failures) != 0 {
		t.Fatalf("unexpected failures: %+v", store.failures)
	}
}

func TestWorkerRecordsRetryAndDeadLetter(t *testing.T) {
	candidate := testCandidate()
	store := &fakeTraceStore{candidates: []Candidate{candidate}}
	client := &fakeTraceClient{err: errors.New("method not available")}
	worker := newTestWorker(t, store, client)
	if _, err := worker.processBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.failures) != 1 || store.failures[0].status != "retry" ||
		store.failures[0].attempt != 1 {
		t.Fatalf("unexpected retry state: %+v", store.failures)
	}
	store.failures = nil
	store.candidates[0].AttemptCount = 2
	if _, err := worker.processBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.failures) != 1 || store.failures[0].status != "dead_letter" {
		t.Fatalf("unexpected dead-letter state: %+v", store.failures)
	}
}

func newTestWorker(
	t *testing.T,
	store Store,
	client Client,
) *Worker {
	t.Helper()
	worker, err := NewWorker(
		store,
		client,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		WorkerConfig{
			ChainID:            8453,
			BatchSize:          5,
			PollInterval:       time.Second,
			RequestTimeout:     time.Second,
			MinRequestInterval: 0,
			MaxAttempts:        3,
			RetryBase:          time.Second,
			RetryMax:           time.Minute,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

type fakeTraceClient struct {
	root CallFrame
	err  error
}

func (f *fakeTraceClient) TraceTransaction(
	context.Context,
	string,
) (CallFrame, json.RawMessage, error) {
	return f.root, json.RawMessage(`{"type":"CALL"}`), f.err
}

type traceFailure struct {
	attempt uint32
	status  string
}

type fakeTraceStore struct {
	candidates []Candidate
	results    []Result
	failures   []traceFailure
}

func (f *fakeTraceStore) TraceCandidates(
	context.Context,
	string,
	uint64,
	uint64,
	int,
) ([]Candidate, error) {
	return f.candidates, nil
}

func (f *fakeTraceStore) InsertTrace(_ context.Context, result Result) error {
	f.results = append(f.results, result)
	return nil
}

func (f *fakeTraceStore) RecordTraceFailure(
	_ context.Context,
	_ Candidate,
	_ string,
	attempt uint32,
	_ time.Time,
	status string,
	_ string,
) error {
	f.failures = append(f.failures, traceFailure{attempt: attempt, status: status})
	return nil
}
