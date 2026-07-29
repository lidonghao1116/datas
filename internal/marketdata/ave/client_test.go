package ave

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/basewatch/base-analytics/internal/marketdata"
)

func TestClientMapsMarketAndRiskResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-API-KEY") != "test-key" {
			http.Error(writer, "missing API key", http.StatusUnauthorized)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/v2/tokens/price":
			fmt.Fprint(writer, `{
				"status":1,"msg":"SUCCESS","data":{
					"0x1111111111111111111111111111111111111111-base":{
						"current_price_usd":"1.25","price_change_24h":"2.5",
						"tvl":"1000","market_cap":"2000","fdv":"3000",
						"tx_volume_u_24h":"400","holders":42,"updated_at":1700000000
					}
				}
			}`)
		case "/v2/contracts/0x1111111111111111111111111111111111111111-base":
			fmt.Fprint(writer, `{
				"status":1,"msg":"SUCCESS","data":{
					"analysis_risk_score":"15","is_honeypot":"0",
					"has_mint_method":true,"has_black_method":null,
					"is_proxy":"1","owner":"0xowner","buy_tax":"1.5","sell_tax":"2"
				}
			}`)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "test-key", time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	token := marketdata.Token{
		ChainID: 8453,
		Address: "0x1111111111111111111111111111111111111111",
	}
	markets, err := client.MarketSnapshots(context.Background(), []marketdata.Token{token})
	if err != nil {
		t.Fatal(err)
	}
	if len(markets) != 1 {
		t.Fatalf("expected one market snapshot, got %d", len(markets))
	}
	if markets[0].PriceUSDRaw != "1.25" || markets[0].Holders != 42 {
		t.Fatalf("unexpected market snapshot %+v", markets[0])
	}
	if markets[0].SourceUpdatedAt.Unix() != 1700000000 {
		t.Fatalf("unexpected source timestamp %s", markets[0].SourceUpdatedAt)
	}

	risk, err := client.RiskSnapshot(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	if risk.IsHoneypot == nil || *risk.IsHoneypot != 0 {
		t.Fatalf("unexpected honeypot value %v", risk.IsHoneypot)
	}
	if risk.HasMintMethod == nil || *risk.HasMintMethod != 1 {
		t.Fatalf("unexpected mint value %v", risk.HasMintMethod)
	}
	if risk.HasBlackMethod != nil {
		t.Fatalf("expected unknown black-method value, got %v", risk.HasBlackMethod)
	}
	if risk.IsProxy == nil || *risk.IsProxy != 1 {
		t.Fatalf("unexpected proxy value %v", risk.IsProxy)
	}
}

func TestClientRejectsOversizedPriceBatch(t *testing.T) {
	client, err := NewClient("https://example.com", "test-key", time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	tokens := make([]marketdata.Token, maxPriceBatch+1)
	if _, err := client.MarketSnapshots(context.Background(), tokens); err == nil {
		t.Fatal("expected oversized batch error")
	}
}
