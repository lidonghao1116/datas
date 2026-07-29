package ave

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/basewatch/base-analytics/internal/marketdata"
)

const (
	SourceName    = "ave"
	maxPriceBatch = 200
)

type Client struct {
	baseURL     *url.URL
	apiKey      string
	httpClient  *http.Client
	minInterval time.Duration

	rateMu   sync.Mutex
	nextCall time.Time
}

func NewClient(
	baseURL, apiKey string,
	timeout, minInterval time.Duration,
) (*Client, error) {
	parsedURL, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("parse AVE base URL: %w", err)
	}
	if parsedURL.Scheme != "https" && parsedURL.Scheme != "http" {
		return nil, fmt.Errorf("AVE base URL must use HTTP or HTTPS")
	}
	if strings.TrimSpace(apiKey) == "" {
		return nil, fmt.Errorf("AVE API key is required")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("AVE request timeout must be positive")
	}
	return &Client{
		baseURL:     parsedURL,
		apiKey:      apiKey,
		httpClient:  &http.Client{Timeout: timeout},
		minInterval: minInterval,
	}, nil
}

func (c *Client) MarketSnapshots(
	ctx context.Context,
	tokens []marketdata.Token,
) ([]marketdata.MarketSnapshot, error) {
	if len(tokens) == 0 {
		return nil, nil
	}
	if len(tokens) > maxPriceBatch {
		return nil, fmt.Errorf("AVE price batch exceeds %d tokens", maxPriceBatch)
	}
	tokenByID := make(map[string]marketdata.Token, len(tokens))
	tokenIDs := make([]string, 0, len(tokens))
	for _, token := range tokens {
		id := tokenID(token)
		tokenIDs = append(tokenIDs, id)
		tokenByID[id] = token
	}
	body, err := json.Marshal(map[string]any{
		"token_ids":         tokenIDs,
		"tvl_min":           0,
		"tx_24h_volume_min": 0,
	})
	if err != nil {
		return nil, fmt.Errorf("encode AVE price request: %w", err)
	}
	var response apiResponse[map[string]priceData]
	if err := c.do(ctx, http.MethodPost, "/v2/tokens/price", body, &response); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	snapshots := make([]marketdata.MarketSnapshot, 0, len(response.Data))
	for id, value := range response.Data {
		token, exists := tokenByID[strings.ToLower(id)]
		if !exists {
			token, exists = tokenByID[id]
		}
		if !exists {
			continue
		}
		sourceUpdatedAt := time.Unix(value.UpdatedAt, 0).UTC()
		if value.UpdatedAt <= 0 {
			sourceUpdatedAt = now
		}
		snapshots = append(snapshots, marketdata.MarketSnapshot{
			Token:             token,
			Source:            SourceName,
			PriceUSDRaw:       value.CurrentPriceUSD,
			PriceChange24hRaw: value.PriceChange24h,
			TVLUSDRaw:         value.TVL,
			MarketCapUSDRaw:   value.MarketCap,
			FDVUSDRaw:         value.FDV,
			Volume24hUSDRaw:   value.Volume24hUSD,
			Holders:           value.Holders,
			SourceUpdatedAt:   sourceUpdatedAt,
			FetchedAt:         now,
		})
	}
	return snapshots, nil
}

func (c *Client) RiskSnapshot(
	ctx context.Context,
	token marketdata.Token,
) (marketdata.RiskSnapshot, error) {
	var response apiResponse[json.RawMessage]
	path := "/v2/contracts/" + tokenID(token)
	if err := c.do(ctx, http.MethodGet, path, nil, &response); err != nil {
		return marketdata.RiskSnapshot{}, err
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(response.Data, &value); err != nil {
		return marketdata.RiskSnapshot{}, fmt.Errorf("decode AVE risk data: %w", err)
	}
	now := time.Now().UTC()
	return marketdata.RiskSnapshot{
		Token:           token,
		Source:          SourceName,
		RiskScoreRaw:    rawString(value["analysis_risk_score"]),
		IsHoneypot:      boolish(value["is_honeypot"]),
		HasMintMethod:   boolish(value["has_mint_method"]),
		HasBlackMethod:  boolish(value["has_black_method"]),
		IsProxy:         boolish(value["is_proxy"]),
		OwnerAddress:    rawString(value["owner"]),
		BuyTaxRaw:       rawString(value["buy_tax"]),
		SellTaxRaw:      rawString(value["sell_tax"]),
		RawJSON:         response.Data,
		SourceUpdatedAt: now,
		FetchedAt:       now,
	}, nil
}

func (c *Client) do(
	ctx context.Context,
	method, path string,
	body []byte,
	target any,
) error {
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := c.waitForTurn(ctx); err != nil {
			return err
		}
		endpoint := c.baseURL.ResolveReference(&url.URL{Path: path})
		request, err := http.NewRequestWithContext(
			ctx,
			method,
			endpoint.String(),
			bytes.NewReader(body),
		)
		if err != nil {
			return fmt.Errorf("create AVE request: %w", err)
		}
		request.Header.Set("X-API-KEY", c.apiKey)
		if len(body) > 0 {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := c.httpClient.Do(request)
		if err != nil {
			lastErr = err
			continue
		}
		payload, readErr := io.ReadAll(io.LimitReader(response.Body, 4*1024*1024))
		response.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read AVE response: %w", readErr)
		}
		if response.StatusCode == http.StatusTooManyRequests ||
			response.StatusCode >= http.StatusInternalServerError {
			lastErr = fmt.Errorf("AVE HTTP %d", response.StatusCode)
			if attempt < maxAttempts-1 {
				if err := wait(ctx, time.Duration(attempt+1)*time.Second); err != nil {
					return err
				}
				continue
			}
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return fmt.Errorf("AVE HTTP %d: %s", response.StatusCode, safeMessage(payload))
		}
		if err := json.Unmarshal(payload, target); err != nil {
			return fmt.Errorf("decode AVE response: %w", err)
		}
		switch typed := target.(type) {
		case *apiResponse[map[string]priceData]:
			if typed.Status != 1 {
				return fmt.Errorf("AVE API error: %s", typed.Message)
			}
		case *apiResponse[json.RawMessage]:
			if typed.Status != 1 {
				return fmt.Errorf("AVE API error: %s", typed.Message)
			}
		}
		return nil
	}
	return fmt.Errorf("AVE request failed: %w", lastErr)
}

func (c *Client) waitForTurn(ctx context.Context) error {
	c.rateMu.Lock()
	now := time.Now()
	delay := c.nextCall.Sub(now)
	if delay < 0 {
		delay = 0
	}
	c.nextCall = now.Add(delay).Add(c.minInterval)
	c.rateMu.Unlock()
	return wait(ctx, delay)
}

type apiResponse[T any] struct {
	Status  int    `json:"status"`
	Message string `json:"msg"`
	Data    T      `json:"data"`
}

type priceData struct {
	CurrentPriceUSD string `json:"current_price_usd"`
	PriceChange24h  string `json:"price_change_24h"`
	TVL             string `json:"tvl"`
	MarketCap       string `json:"market_cap"`
	FDV             string `json:"fdv"`
	Volume24hUSD    string `json:"tx_volume_u_24h"`
	Holders         uint64 `json:"holders"`
	UpdatedAt       int64  `json:"updated_at"`
}

func tokenID(token marketdata.Token) string {
	chain := "base"
	if token.ChainID != 8453 {
		chain = strconv.FormatUint(token.ChainID, 10)
	}
	return strings.ToLower(token.Address) + "-" + chain
}

func rawString(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	return strings.Trim(string(raw), `"`)
}

func boolish(raw json.RawMessage) *uint8 {
	value := strings.ToLower(strings.TrimSpace(rawString(raw)))
	var normalized uint8
	switch value {
	case "1", "true", "yes":
		normalized = 1
	case "0", "false", "no":
	default:
		return nil
	}
	return &normalized
}

func safeMessage(payload []byte) string {
	message := strings.TrimSpace(string(payload))
	if len(message) > 256 {
		return message[:256]
	}
	return message
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
