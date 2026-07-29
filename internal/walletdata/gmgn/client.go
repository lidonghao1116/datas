package gmgn

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/basewatch/base-analytics/internal/walletenrichment"
)

const walletStatsPath = "/v1/user/wallet_stats"

type Client struct {
	baseURL     *url.URL
	apiKey      string
	httpClient  *http.Client
	minInterval time.Duration
	mu          sync.Mutex
	nextRequest time.Time
}

func NewClient(
	baseURL string,
	apiKey string,
	timeout time.Duration,
	minInterval time.Duration,
) (*Client, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid GMGN base URL")
	}
	if strings.TrimSpace(apiKey) == "" {
		return nil, fmt.Errorf("GMGN API key is required")
	}
	if timeout <= 0 || minInterval < 0 {
		return nil, fmt.Errorf("GMGN timeout must be positive and interval non-negative")
	}
	return &Client{
		baseURL:     parsed,
		apiKey:      apiKey,
		httpClient:  &http.Client{Timeout: timeout},
		minInterval: minInterval,
	}, nil
}

func (c *Client) WalletStats(
	ctx context.Context,
	chain string,
	walletAddress string,
	period string,
) (walletenrichment.Stats, error) {
	if chain != "base" {
		return walletenrichment.Stats{}, fmt.Errorf("unsupported GMGN chain %q", chain)
	}
	if !common.IsHexAddress(walletAddress) {
		return walletenrichment.Stats{}, fmt.Errorf("invalid GMGN wallet address")
	}
	if period != "7d" && period != "30d" {
		return walletenrichment.Stats{}, fmt.Errorf("unsupported GMGN period %q", period)
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := c.waitForRateLimit(ctx); err != nil {
			return walletenrichment.Stats{}, err
		}
		stats, retryAfter, err := c.walletStatsOnce(
			ctx,
			chain,
			strings.ToLower(common.HexToAddress(walletAddress).Hex()),
			period,
		)
		if err == nil {
			return stats, nil
		}
		lastErr = err
		if retryAfter <= 0 || attempt == 1 {
			break
		}
		if retryAfter > 10*time.Second {
			break
		}
		if err := wait(ctx, retryAfter); err != nil {
			return walletenrichment.Stats{}, err
		}
	}
	return walletenrichment.Stats{}, lastErr
}

func (c *Client) walletStatsOnce(
	ctx context.Context,
	chain string,
	walletAddress string,
	period string,
) (walletenrichment.Stats, time.Duration, error) {
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + walletStatsPath
	query := endpoint.Query()
	query.Set("chain", chain)
	query.Set("wallet_address", walletAddress)
	query.Set("period", period)
	query.Set("timestamp", strconv.FormatInt(time.Now().Unix(), 10))
	query.Set("client_id", randomClientID())
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return walletenrichment.Stats{}, 0, fmt.Errorf("create GMGN wallet stats request: %w", err)
	}
	request.Header.Set("X-APIKEY", c.apiKey)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "base-analytics/1.0")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return walletenrichment.Stats{}, 0, fmt.Errorf("request GMGN wallet stats: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return walletenrichment.Stats{}, 0, fmt.Errorf("read GMGN wallet stats: %w", err)
	}
	if response.StatusCode == http.StatusTooManyRequests {
		return walletenrichment.Stats{}, retryDelay(response), fmt.Errorf("GMGN rate limit exceeded")
	}
	if response.StatusCode >= 500 {
		return walletenrichment.Stats{}, time.Second, fmt.Errorf(
			"GMGN wallet stats unavailable: HTTP %d",
			response.StatusCode,
		)
	}
	if response.StatusCode != http.StatusOK {
		return walletenrichment.Stats{}, 0, fmt.Errorf(
			"GMGN wallet stats failed: HTTP %d",
			response.StatusCode,
		)
	}

	var envelope struct {
		Code    json.RawMessage `json:"code"`
		Message string          `json:"message"`
		Error   string          `json:"error"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return walletenrichment.Stats{}, 0, fmt.Errorf("decode GMGN wallet stats envelope: %w", err)
	}
	if normalizeCode(envelope.Code) != "0" {
		message := strings.TrimSpace(envelope.Error + " " + envelope.Message)
		return walletenrichment.Stats{}, 0, fmt.Errorf(
			"GMGN wallet stats API error: %s",
			truncate(message, 300),
		)
	}
	stats, err := decodeStats(envelope.Data, walletAddress, period)
	if err != nil {
		return walletenrichment.Stats{}, 0, err
	}
	return stats, 0, nil
}

func (c *Client) waitForRateLimit(ctx context.Context) error {
	c.mu.Lock()
	now := time.Now()
	waitFor := time.Duration(0)
	if now.Before(c.nextRequest) {
		waitFor = c.nextRequest.Sub(now)
		now = c.nextRequest
	}
	c.nextRequest = now.Add(c.minInterval)
	c.mu.Unlock()
	return wait(ctx, waitFor)
}

func wait(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryDelay(response *http.Response) time.Duration {
	if raw := strings.TrimSpace(response.Header.Get("Retry-After")); raw != "" {
		if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	if raw := strings.TrimSpace(response.Header.Get("X-RateLimit-Reset")); raw != "" {
		if resetAt, err := strconv.ParseInt(raw, 10, 64); err == nil {
			delay := time.Until(time.Unix(resetAt, 0)) + time.Second
			if delay > 0 {
				return delay
			}
		}
	}
	return time.Second
}

func randomClientID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(value[:])
}

func normalizeCode(raw json.RawMessage) string {
	value := strings.TrimSpace(string(raw))
	return strings.Trim(value, `"`)
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
