package alerting

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWebhookSenderUsesIdempotencyAndHMAC(t *testing.T) {
	var receivedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Idempotency-Key") != "alert-1" {
			t.Errorf("unexpected idempotency key %q", request.Header.Get("Idempotency-Key"))
		}
		receivedBody, _ = io.ReadAll(request.Body)
		mac := hmac.New(sha256.New, []byte("secret"))
		mac.Write(receivedBody)
		expected := hex.EncodeToString(mac.Sum(nil))
		if request.Header.Get("X-Alert-Signature-SHA256") != expected {
			t.Error("unexpected webhook signature")
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sender, err := NewWebhookSender(server.URL, "secret", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Send(context.Background(), Delivery{
		Alert: Alert{Key: "alert-1", Type: "large_buy"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(receivedBody) == 0 {
		t.Fatal("expected webhook body")
	}
}

func TestWebhookSenderReturnsNon2xxError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "retry later", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	sender, err := NewWebhookSender(server.URL, "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Send(context.Background(), Delivery{
		Alert: Alert{Key: "alert-2"},
	}); err == nil {
		t.Fatal("expected webhook error")
	}
}
