package alerting

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestDispatcherAcknowledgesSuccessAndSchedulesFailure(t *testing.T) {
	store := &fakeDeliveryStore{deliveries: []Delivery{
		{Alert: Alert{Key: "success"}, Attempt: 1},
		{Alert: Alert{Key: "failure"}, Attempt: 2},
	}}
	dispatcher, err := NewDispatcher(
		store,
		fakeSender{failureKey: "failure"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"worker-1",
		10,
		time.Minute,
		time.Second,
		time.Second,
		3,
		5*time.Second,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	claimed, delivered, failed, err := dispatcher.process(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if claimed != 2 || delivered != 1 || failed != 1 {
		t.Fatalf("unexpected result %d/%d/%d", claimed, delivered, failed)
	}
	if len(store.delivered) != 1 || store.delivered[0] != "success" {
		t.Fatalf("unexpected acknowledgements %v", store.delivered)
	}
	if len(store.failed) != 1 || store.failed[0].deadLetter {
		t.Fatalf("unexpected failures %+v", store.failed)
	}
	if delay := time.Until(store.failed[0].nextAttempt); delay < 9*time.Second {
		t.Fatalf("expected exponential retry delay, got %s", delay)
	}
}

func TestDispatcherMovesExhaustedDeliveryToDeadLetter(t *testing.T) {
	store := &fakeDeliveryStore{deliveries: []Delivery{
		{Alert: Alert{Key: "dead"}, Attempt: 3},
	}}
	dispatcher, err := NewDispatcher(
		store,
		fakeSender{failureKey: "dead"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"worker-1",
		1,
		time.Minute,
		time.Second,
		time.Second,
		3,
		time.Second,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = dispatcher.process(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(store.failed) != 1 || !store.failed[0].deadLetter {
		t.Fatalf("expected dead letter, got %+v", store.failed)
	}
}

func TestRetryDelayIsCapped(t *testing.T) {
	if delay := retryDelay(time.Second, 10*time.Second, 20); delay != 10*time.Second {
		t.Fatalf("expected capped delay, got %s", delay)
	}
}

type failedDelivery struct {
	key         string
	nextAttempt time.Time
	deadLetter  bool
}

type fakeDeliveryStore struct {
	deliveries []Delivery
	delivered  []string
	failed     []failedDelivery
}

func (s *fakeDeliveryStore) ClaimDeliveries(
	context.Context,
	string,
	int,
	time.Duration,
) ([]Delivery, error) {
	return s.deliveries, nil
}

func (s *fakeDeliveryStore) MarkDelivered(
	_ context.Context,
	alertKey, _ string,
) error {
	s.delivered = append(s.delivered, alertKey)
	return nil
}

func (s *fakeDeliveryStore) MarkFailed(
	_ context.Context,
	alertKey, _, _ string,
	nextAttemptAt time.Time,
	deadLetter bool,
) error {
	s.failed = append(s.failed, failedDelivery{
		key:         alertKey,
		nextAttempt: nextAttemptAt,
		deadLetter:  deadLetter,
	})
	return nil
}

type fakeSender struct {
	failureKey string
}

func (s fakeSender) Send(_ context.Context, delivery Delivery) error {
	if delivery.Key == s.failureKey {
		return errors.New("delivery failed")
	}
	return nil
}
