package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

const (
	defaultPageLimit = 50
	maxPageLimit     = 200
	alertPollBatch   = 200
)

type Server struct {
	alerts         AlertStore
	analytics      AnalyticsStore
	logger         *slog.Logger
	hub            *Hub
	pollInterval   time.Duration
	allowedOrigins map[string]struct{}
	allowAnyOrigin bool
}

func NewServer(
	alerts AlertStore,
	analytics AnalyticsStore,
	logger *slog.Logger,
	pollInterval time.Duration,
	allowedOrigins []string,
) (*Server, error) {
	if alerts == nil || analytics == nil {
		return nil, fmt.Errorf("API alert and analytics stores are required")
	}
	if pollInterval <= 0 {
		return nil, fmt.Errorf("API alert poll interval must be positive")
	}
	origins := make(map[string]struct{}, len(allowedOrigins))
	allowAny := false
	for _, origin := range allowedOrigins {
		origin = strings.TrimSpace(origin)
		if origin == "*" {
			allowAny = true
			continue
		}
		if origin != "" {
			origins[origin] = struct{}{}
		}
	}
	return &Server{
		alerts:         alerts,
		analytics:      analytics,
		logger:         logger,
		hub:            NewHub(logger),
		pollInterval:   pollInterval,
		allowedOrigins: origins,
		allowAnyOrigin: allowAny,
	}, nil
}

func (s *Server) Handler(ctx context.Context) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/api/v1/alerts", s.recentAlerts)
	mux.HandleFunc("/api/v1/trades/large", s.recentLargeTrades)
	mux.HandleFunc("/api/v1/tokens/", s.token)
	mux.HandleFunc("/api/v1/wallets/", s.wallet)
	mux.HandleFunc("/api/v1/transactions/", s.transactionTrace)
	mux.HandleFunc("/ws/alerts", s.websocketAlerts(ctx))
	return s.cors(mux)
}

func (s *Server) transactionTrace(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer)
		return
	}
	path := strings.TrimPrefix(request.URL.Path, "/api/v1/transactions/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[1] != "trace" {
		writeError(writer, http.StatusBadRequest, "invalid transaction trace resource")
		return
	}
	hashBytes, err := hexutil.Decode(parts[0])
	if err != nil || len(hashBytes) != common.HashLength {
		writeError(writer, http.StatusBadRequest, "invalid transaction hash")
		return
	}
	hash := strings.ToLower(common.BytesToHash(hashBytes).Hex())
	trace, err := s.analytics.TransactionTrace(request.Context(), 8453, hash)
	if errors.Is(err, ErrNotFound) {
		writeError(writer, http.StatusNotFound, "transaction trace not found")
		return
	}
	if err != nil {
		s.logger.Error("query transaction trace", "transaction_hash", hash, "error", err)
		writeError(writer, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": trace})
}

func (s *Server) Run(ctx context.Context, address string) error {
	httpServer := &http.Server{
		Addr:              address,
		Handler:           s.Handler(ctx),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go s.pollNewAlerts(ctx)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		httpServer.Shutdown(shutdownCtx)
	}()
	s.logger.Info("query API listening", "address", address)
	err := httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) health(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	if err := s.alerts.Ping(ctx); err != nil {
		writeError(writer, http.StatusServiceUnavailable, "postgres unavailable")
		return
	}
	if err := s.analytics.Ping(ctx); err != nil {
		writeError(writer, http.StatusServiceUnavailable, "clickhouse unavailable")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"status":            "ok",
		"websocket_clients": s.hub.ClientCount(),
	})
}

func (s *Server) recentAlerts(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer)
		return
	}
	limit, err := queryLimit(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	alerts, err := s.alerts.RecentAlerts(request.Context(), AlertFilter{
		Status:   strings.TrimSpace(request.URL.Query().Get("status")),
		Severity: strings.TrimSpace(request.URL.Query().Get("severity")),
		Type:     strings.TrimSpace(request.URL.Query().Get("type")),
		Limit:    limit,
	})
	if err != nil {
		s.logger.Error("query recent alerts", "error", err)
		writeError(writer, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": alerts})
}

func (s *Server) recentLargeTrades(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer)
		return
	}
	limit, err := queryLimit(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	trades, err := s.analytics.RecentLargeTrades(request.Context(), limit)
	if err != nil {
		s.logger.Error("query recent large trades", "error", err)
		writeError(writer, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": trades})
}

func (s *Server) token(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer)
		return
	}
	path := strings.TrimPrefix(request.URL.Path, "/api/v1/tokens/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || !common.IsHexAddress(parts[0]) {
		writeError(writer, http.StatusBadRequest, "invalid token address or resource")
		return
	}
	normalized := strings.ToLower(common.HexToAddress(parts[0]).Hex())
	switch parts[1] {
	case "market":
		s.tokenMarket(writer, request, normalized)
	case "dev":
		s.tokenDevProfile(writer, request, normalized)
	default:
		writeError(writer, http.StatusBadRequest, "invalid token resource")
	}
}

func (s *Server) tokenMarket(
	writer http.ResponseWriter,
	request *http.Request,
	normalized string,
) {
	market, err := s.analytics.TokenMarket(request.Context(), 8453, normalized)
	if errors.Is(err, ErrNotFound) {
		writeError(writer, http.StatusNotFound, "token market not found")
		return
	}
	if err != nil {
		s.logger.Error("query token market", "token_address", normalized, "error", err)
		writeError(writer, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": market})
}

func (s *Server) tokenDevProfile(
	writer http.ResponseWriter,
	request *http.Request,
	address string,
) {
	profile, err := s.analytics.TokenDevProfile(request.Context(), 8453, address)
	if errors.Is(err, ErrNotFound) {
		writeError(writer, http.StatusNotFound, "Token Dev profile not found")
		return
	}
	if err != nil {
		s.logger.Error("query Token Dev profile", "token_address", address, "error", err)
		writeError(writer, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": profile})
}

func (s *Server) wallet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer)
		return
	}
	path := strings.TrimPrefix(request.URL.Path, "/api/v1/wallets/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 ||
		!common.IsHexAddress(parts[0]) ||
		common.HexToAddress(parts[0]) == (common.Address{}) {
		writeError(writer, http.StatusBadRequest, "invalid wallet address or resource")
		return
	}
	address := strings.ToLower(common.HexToAddress(parts[0]).Hex())
	switch parts[1] {
	case "profile":
		s.walletProfile(writer, request, address)
	case "trades":
		s.walletTrades(writer, request, address)
	case "positions":
		s.walletPositions(writer, request, address)
	case "score":
		s.walletScore(writer, request, address)
	case "pnl":
		s.walletPnL(writer, request, address)
	default:
		writeError(writer, http.StatusBadRequest, "invalid wallet resource")
	}
}

func (s *Server) walletProfile(
	writer http.ResponseWriter,
	request *http.Request,
	address string,
) {
	profile, err := s.analytics.WalletProfile(request.Context(), 8453, address)
	if errors.Is(err, ErrNotFound) {
		writeError(writer, http.StatusNotFound, "wallet profile not found")
		return
	}
	if err != nil {
		s.logger.Error("query wallet profile", "wallet_address", address, "error", err)
		writeError(writer, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": profile})
}

func (s *Server) walletTrades(
	writer http.ResponseWriter,
	request *http.Request,
	address string,
) {
	limit, err := queryLimit(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	trades, err := s.analytics.WalletTrades(request.Context(), 8453, address, limit)
	if err != nil {
		s.logger.Error("query wallet trades", "wallet_address", address, "error", err)
		writeError(writer, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": trades})
}

func (s *Server) walletPositions(
	writer http.ResponseWriter,
	request *http.Request,
	address string,
) {
	limit, err := queryLimit(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	positions, err := s.analytics.WalletPositions(request.Context(), 8453, address, limit)
	if err != nil {
		s.logger.Error("query wallet positions", "wallet_address", address, "error", err)
		writeError(writer, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": positions})
}

func (s *Server) walletScore(
	writer http.ResponseWriter,
	request *http.Request,
	address string,
) {
	score, err := s.analytics.WalletSmartScore(request.Context(), 8453, address)
	if errors.Is(err, ErrNotFound) {
		writeError(writer, http.StatusNotFound, "wallet score not found")
		return
	}
	if err != nil {
		s.logger.Error("query wallet score", "wallet_address", address, "error", err)
		writeError(writer, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": score})
}

func (s *Server) walletPnL(
	writer http.ResponseWriter,
	request *http.Request,
	address string,
) {
	limit, err := queryLimit(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	score, err := s.analytics.WalletSmartScore(request.Context(), 8453, address)
	if errors.Is(err, ErrNotFound) {
		writeError(writer, http.StatusNotFound, "wallet PnL not found")
		return
	}
	if err != nil {
		s.logger.Error("query wallet PnL summary", "wallet_address", address, "error", err)
		writeError(writer, http.StatusInternalServerError, "query failed")
		return
	}
	tokens, err := s.analytics.WalletTokenPnL(request.Context(), 8453, address, limit)
	if err != nil {
		s.logger.Error("query wallet token PnL", "wallet_address", address, "error", err)
		writeError(writer, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"data": map[string]any{
			"summary": score,
			"tokens":  tokens,
		},
	})
}

func (s *Server) websocketAlerts(ctx context.Context) http.HandlerFunc {
	upgrader := newUpgrader(s.originAllowed)
	return func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			methodNotAllowed(writer)
			return
		}
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		client := &websocketClient{
			hub:  s.hub,
			conn: connection,
			send: make(chan []byte, websocketQueueSize),
		}
		s.hub.register(client)
		go client.writePump(ctx)
		client.readPump()
	}
}

func (s *Server) pollNewAlerts(ctx context.Context) {
	cursor := AlertCursor{CreatedAt: time.Now().UTC()}
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				alerts, err := s.alerts.AlertsAfter(ctx, cursor, alertPollBatch)
				if err != nil {
					s.logger.Error("poll WebSocket alerts", "error", err)
					break
				}
				for _, alert := range alerts {
					s.hub.BroadcastAlert(alert)
					cursor = AlertCursor{CreatedAt: alert.CreatedAt, Key: alert.Key}
				}
				if len(alerts) < alertPollBatch {
					break
				}
			}
		}
	}
}

func (s *Server) originAllowed(request *http.Request) bool {
	origin := strings.TrimSpace(request.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	if s.allowAnyOrigin {
		return true
	}
	_, exists := s.allowedOrigins[origin]
	return exists
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		origin := strings.TrimSpace(request.Header.Get("Origin"))
		if origin != "" && s.originAllowed(request) {
			writer.Header().Set("Access-Control-Allow-Origin", origin)
			writer.Header().Set("Vary", "Origin")
			writer.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			writer.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		}
		if request.Method == http.MethodOptions {
			if origin != "" && !s.originAllowed(request) {
				writeError(writer, http.StatusForbidden, "origin not allowed")
				return
			}
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func queryLimit(request *http.Request) (int, error) {
	raw := strings.TrimSpace(request.URL.Query().Get("limit"))
	if raw == "" {
		return defaultPageLimit, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return 0, fmt.Errorf("limit must be a positive integer")
	}
	if limit > maxPageLimit {
		limit = maxPageLimit
	}
	return limit, nil
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]string{"error": message})
}

func methodNotAllowed(writer http.ResponseWriter) {
	writeError(writer, http.StatusMethodNotAllowed, "method not allowed")
}
