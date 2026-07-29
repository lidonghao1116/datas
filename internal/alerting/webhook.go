package alerting

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type WebhookSender struct {
	endpoint   *url.URL
	secret     []byte
	httpClient *http.Client
}

func NewWebhookSender(endpoint, secret string, timeout time.Duration) (*WebhookSender, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse alert webhook URL: %w", err)
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return nil, fmt.Errorf("alert webhook URL must use HTTP or HTTPS")
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("alert webhook URL must include a host")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("webhook timeout must be positive")
	}
	return &WebhookSender{
		endpoint:   parsed,
		secret:     []byte(secret),
		httpClient: &http.Client{Timeout: timeout},
	}, nil
}

func (s *WebhookSender) Send(ctx context.Context, delivery Delivery) error {
	body, err := json.Marshal(delivery)
	if err != nil {
		return fmt.Errorf("encode webhook delivery: %w", err)
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		s.endpoint.String(),
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create webhook request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", delivery.Key)
	request.Header.Set("X-Alert-Key", delivery.Key)
	if len(s.secret) > 0 {
		mac := hmac.New(sha256.New, s.secret)
		mac.Write(body)
		request.Header.Set("X-Alert-Signature-SHA256", hex.EncodeToString(mac.Sum(nil)))
	}
	response, err := s.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("send webhook: %w", err)
	}
	defer response.Body.Close()
	payload, readErr := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if readErr != nil {
		return fmt.Errorf("read webhook response: %w", readErr)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message := strings.TrimSpace(string(payload))
		if len(message) > 256 {
			message = message[:256]
		}
		return fmt.Errorf("webhook HTTP %d: %s", response.StatusCode, message)
	}
	return nil
}
