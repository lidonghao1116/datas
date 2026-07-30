package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type fakeAlertStore struct {
	mu           sync.Mutex
	recent       []Alert
	after        []Alert
	recentFilter AlertFilter
	pingErr      error
}

func (s *fakeAlertStore) RecentAlerts(_ context.Context, filter AlertFilter) ([]Alert, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recentFilter = filter
	return s.recent, nil
}

func (s *fakeAlertStore) AlertsAfter(
	_ context.Context,
	_ AlertCursor,
	_ int,
) ([]Alert, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	alerts := s.after
	s.after = nil
	return alerts, nil
}

func (s *fakeAlertStore) Ping(context.Context) error {
	return s.pingErr
}

type fakeAnalyticsStore struct {
	trades        []LargeTrade
	market        TokenMarket
	marketErr     error
	profile       WalletProfile
	profileErr    error
	walletTrades  []WalletTrade
	positions     []WalletPosition
	score         WalletSmartScore
	scoreErr      error
	tokenPnL      []WalletTokenPnL
	trace         TransactionTrace
	traceErr      error
	tradeLimit    int
	walletLimit   int
	tokenAddress  string
	walletAddress string
	pingErr       error
}

func (s *fakeAnalyticsStore) RecentLargeTrades(
	_ context.Context,
	limit int,
) ([]LargeTrade, error) {
	s.tradeLimit = limit
	return s.trades, nil
}

func (s *fakeAnalyticsStore) TokenMarket(
	_ context.Context,
	_ uint64,
	address string,
) (TokenMarket, error) {
	s.tokenAddress = address
	return s.market, s.marketErr
}

func (s *fakeAnalyticsStore) WalletProfile(
	_ context.Context,
	_ uint64,
	address string,
) (WalletProfile, error) {
	s.walletAddress = address
	return s.profile, s.profileErr
}

func (s *fakeAnalyticsStore) WalletTrades(
	_ context.Context,
	_ uint64,
	address string,
	limit int,
) ([]WalletTrade, error) {
	s.walletAddress = address
	s.walletLimit = limit
	return s.walletTrades, nil
}

func (s *fakeAnalyticsStore) WalletPositions(
	_ context.Context,
	_ uint64,
	address string,
	limit int,
) ([]WalletPosition, error) {
	s.walletAddress = address
	s.walletLimit = limit
	return s.positions, nil
}

func (s *fakeAnalyticsStore) WalletSmartScore(
	_ context.Context,
	_ uint64,
	address string,
) (WalletSmartScore, error) {
	s.walletAddress = address
	return s.score, s.scoreErr
}

func (s *fakeAnalyticsStore) WalletTokenPnL(
	_ context.Context,
	_ uint64,
	address string,
	limit int,
) ([]WalletTokenPnL, error) {
	s.walletAddress = address
	s.walletLimit = limit
	return s.tokenPnL, nil
}

func (s *fakeAnalyticsStore) TransactionTrace(
	_ context.Context,
	_ uint64,
	transactionHash string,
) (TransactionTrace, error) {
	s.trace.TransactionHash = transactionHash
	return s.trace, s.traceErr
}

func (s *fakeAnalyticsStore) Ping(context.Context) error {
	return s.pingErr
}

func newTestServer(t *testing.T, alerts AlertStore, analytics AnalyticsStore) *Server {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := NewServer(
		alerts,
		analytics,
		logger,
		10*time.Millisecond,
		[]string{"http://localhost:3000"},
	)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return server
}

func TestRecentAlertsFiltersAndClampsLimit(t *testing.T) {
	alerts := &fakeAlertStore{
		recent: []Alert{{Key: "alert-1", Payload: json.RawMessage(`{"usd":"10000"}`)}},
	}
	server := newTestServer(t, alerts, &fakeAnalyticsStore{})
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/alerts?status=pending&severity=critical&type=large_buy&limit=999",
		nil,
	)
	response := httptest.NewRecorder()

	server.Handler(context.Background()).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	alerts.mu.Lock()
	filter := alerts.recentFilter
	alerts.mu.Unlock()
	if filter.Limit != maxPageLimit ||
		filter.Status != "pending" ||
		filter.Severity != "critical" ||
		filter.Type != "large_buy" {
		t.Fatalf("unexpected filter: %+v", filter)
	}
}

func TestTransactionTrace(t *testing.T) {
	const hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	analytics := &fakeAnalyticsStore{trace: TransactionTrace{
		TraceVersion: "call-v1",
		FrameCount:   2,
		Calls:        []TransactionCall{{TraceID: hash + ":root"}},
	}}
	server := newTestServer(t, &fakeAlertStore{}, analytics)
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/transactions/"+hash+"/trace",
		nil,
	)
	response := httptest.NewRecorder()
	server.Handler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusOK ||
		!strings.Contains(response.Body.String(), `"trace_version":"call-v1"`) {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if analytics.trace.TransactionHash != hash {
		t.Fatalf("unexpected normalized hash %s", analytics.trace.TransactionHash)
	}
}

func TestTransactionTraceRejectsInvalidHash(t *testing.T) {
	server := newTestServer(t, &fakeAlertStore{}, &fakeAnalyticsStore{})
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/transactions/0x1234/trace",
		nil,
	)
	response := httptest.NewRecorder()
	server.Handler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestTokenMarketRequiresExactRouteAndNormalizesAddress(t *testing.T) {
	const token = "0x4200000000000000000000000000000000000006"
	analytics := &fakeAnalyticsStore{
		market: TokenMarket{ChainID: 8453, TokenAddress: token},
	}
	server := newTestServer(t, &fakeAlertStore{}, analytics)

	valid := httptest.NewRecorder()
	server.Handler(context.Background()).ServeHTTP(
		valid,
		httptest.NewRequest(http.MethodGet, "/api/v1/tokens/"+token+"/market", nil),
	)
	if valid.Code != http.StatusOK {
		t.Fatalf("valid route status = %d, body = %s", valid.Code, valid.Body.String())
	}
	if analytics.tokenAddress != token {
		t.Fatalf("normalized address = %q", analytics.tokenAddress)
	}

	invalid := httptest.NewRecorder()
	server.Handler(context.Background()).ServeHTTP(
		invalid,
		httptest.NewRequest(http.MethodGet, "/api/v1/tokens/"+token, nil),
	)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid route status = %d", invalid.Code)
	}
}

func TestTokenMarketNotFound(t *testing.T) {
	analytics := &fakeAnalyticsStore{marketErr: ErrNotFound}
	server := newTestServer(t, &fakeAlertStore{}, analytics)
	response := httptest.NewRecorder()
	server.Handler(context.Background()).ServeHTTP(
		response,
		httptest.NewRequest(
			http.MethodGet,
			"/api/v1/tokens/0x4200000000000000000000000000000000000006/market",
			nil,
		),
	)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestWalletProfileNormalizesAddress(t *testing.T) {
	const wallet = "0xAbC0000000000000000000000000000000000000"
	analytics := &fakeAnalyticsStore{
		profile: WalletProfile{ChainID: 8453},
	}
	server := newTestServer(t, &fakeAlertStore{}, analytics)
	response := httptest.NewRecorder()
	server.Handler(context.Background()).ServeHTTP(
		response,
		httptest.NewRequest(
			http.MethodGet,
			"/api/v1/wallets/"+wallet+"/profile",
			nil,
		),
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if analytics.walletAddress != strings.ToLower(wallet) {
		t.Fatalf("wallet address = %q", analytics.walletAddress)
	}
}

func TestWalletTradesClampsLimit(t *testing.T) {
	analytics := &fakeAnalyticsStore{}
	server := newTestServer(t, &fakeAlertStore{}, analytics)
	response := httptest.NewRecorder()
	server.Handler(context.Background()).ServeHTTP(
		response,
		httptest.NewRequest(
			http.MethodGet,
			"/api/v1/wallets/0xabc0000000000000000000000000000000000000/trades?limit=999",
			nil,
		),
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if analytics.walletLimit != maxPageLimit {
		t.Fatalf("wallet limit = %d", analytics.walletLimit)
	}
}

func TestWalletPnLReturnsSummaryAndClampsLimit(t *testing.T) {
	analytics := &fakeAnalyticsStore{
		score:    WalletSmartScore{SmartScoreRaw: "72.5"},
		tokenPnL: []WalletTokenPnL{{TokenAddress: "0xtoken"}},
	}
	server := newTestServer(t, &fakeAlertStore{}, analytics)
	response := httptest.NewRecorder()
	server.Handler(context.Background()).ServeHTTP(
		response,
		httptest.NewRequest(
			http.MethodGet,
			"/api/v1/wallets/0xabc0000000000000000000000000000000000000/pnl?limit=999",
			nil,
		),
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if analytics.walletLimit != maxPageLimit {
		t.Fatalf("wallet PnL limit = %d", analytics.walletLimit)
	}
}

func TestHealthReportsDependencyFailure(t *testing.T) {
	server := newTestServer(
		t,
		&fakeAlertStore{pingErr: errors.New("postgres down")},
		&fakeAnalyticsStore{},
	)
	response := httptest.NewRecorder()
	server.Handler(context.Background()).ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/healthz", nil),
	)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestCORSRejectsUnknownPreflightOrigin(t *testing.T) {
	server := newTestServer(t, &fakeAlertStore{}, &fakeAnalyticsStore{})
	request := httptest.NewRequest(http.MethodOptions, "/api/v1/alerts", nil)
	request.Header.Set("Origin", "https://untrusted.example")
	response := httptest.NewRecorder()

	server.Handler(context.Background()).ServeHTTP(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestWebSocketReceivesBroadcastAlert(t *testing.T) {
	server := newTestServer(t, &fakeAlertStore{}, &fakeAnalyticsStore{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	httpServer := httptest.NewServer(server.Handler(ctx))
	defer httpServer.Close()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws/alerts"
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial WebSocket: %v", err)
	}
	defer connection.Close()

	deadline := time.Now().Add(time.Second)
	for server.hub.ClientCount() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.hub.ClientCount() != 1 {
		t.Fatal("WebSocket client was not registered")
	}

	server.hub.BroadcastAlert(Alert{
		Key:       "alert-1",
		Type:      "large_buy",
		Severity:  "critical",
		Payload:   json.RawMessage(`{"usd":"50000"}`),
		CreatedAt: time.Now().UTC(),
	})
	connection.SetReadDeadline(time.Now().Add(time.Second))
	var event struct {
		Type string `json:"type"`
		Data Alert  `json:"data"`
	}
	if err := connection.ReadJSON(&event); err != nil {
		t.Fatalf("read WebSocket event: %v", err)
	}
	if event.Type != "alert" || event.Data.Key != "alert-1" {
		t.Fatalf("unexpected WebSocket event: %+v", event)
	}
}
