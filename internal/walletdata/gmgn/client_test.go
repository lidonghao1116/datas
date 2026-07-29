package gmgn

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWalletStatsParsesOfficialResponseShape(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != walletStatsPath {
			t.Fatalf("path = %q", request.URL.Path)
		}
		if request.Header.Get("X-APIKEY") != "test-key" {
			t.Fatal("missing API key header")
		}
		query := request.URL.Query()
		if query.Get("chain") != "base" ||
			query.Get("period") != "7d" ||
			query.Get("timestamp") == "" ||
			query.Get("client_id") == "" {
			t.Fatalf("unexpected query: %v", query)
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Write([]byte(`{
			"code": 0,
			"data": {
				"wallet_address": "0xabc0000000000000000000000000000000000000",
				"native_balance": "1.25",
				"realized_profit": "120.5",
				"realized_profit_pnl": "0.5",
				"buy": 12,
				"sell": 8,
				"bought_cost": "241",
				"last_timestamp": 1710000000,
				"pnl_stat": {
					"token_num": 7,
					"winrate": "0.6",
					"avg_holding_period": 3600
				},
				"common": {
					"name": "wallet",
					"tags": ["smart_money", "whale"],
					"followers_count": 10,
					"is_blue_verified": true,
					"created_at": 1700000000
				}
			}
		}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "test-key", time.Second, 0)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	stats, err := client.WalletStats(
		context.Background(),
		"base",
		"0xabc0000000000000000000000000000000000000",
		"7d",
	)
	if err != nil {
		t.Fatalf("wallet stats: %v", err)
	}
	if stats.RealizedProfitRaw != "120.5" ||
		stats.PnLRaw != "0.5" ||
		stats.WinRateRaw != "0.6" ||
		stats.BuyCount != 12 ||
		stats.TokenCount != 7 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if stats.Identity.PrimaryTag != "" ||
		len(stats.Identity.Tags) != 2 ||
		!stats.Identity.IsBlueVerified {
		t.Fatalf("unexpected identity: %+v", stats.Identity)
	}
	if stats.SourceUpdatedAt.Unix() != 1710000000 {
		t.Fatalf("source updated at = %v", stats.SourceUpdatedAt)
	}
}

func TestWalletStatsRejectsBusinessError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.Write([]byte(`{"code":1001,"error":"INVALID_REQUEST","message":"bad wallet"}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "test-key", time.Second, 0)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if _, err := client.WalletStats(
		context.Background(),
		"base",
		"0xabc0000000000000000000000000000000000000",
		"7d",
	); err == nil {
		t.Fatal("expected API error")
	}
}
