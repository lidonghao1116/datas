package traceanalytics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRPCClientRequestsCallTracerWithLogs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		var payload struct {
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
			ID     json.RawMessage   `json:"id"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Error(err)
			return
		}
		if payload.Method != "debug_traceTransaction" || len(payload.Params) != 2 {
			t.Errorf("unexpected request: %+v", payload)
		}
		var options struct {
			Tracer       string `json:"tracer"`
			TracerConfig struct {
				WithLog bool `json:"withLog"`
			} `json:"tracerConfig"`
		}
		if err := json.Unmarshal(payload.Params[1], &options); err != nil {
			t.Error(err)
		}
		if options.Tracer != "callTracer" || !options.TracerConfig.WithLog {
			t.Errorf("unexpected trace options: %+v", options)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      json.RawMessage(payload.ID),
			"result": map[string]any{
				"type": "CALL",
				"from": "0x1111111111111111111111111111111111111111",
				"to":   "0x2222222222222222222222222222222222222222",
			},
		})
	}))
	defer server.Close()
	client, err := NewRPCClient(server.URL, "10s")
	if err != nil {
		t.Fatal(err)
	}
	frame, raw, err := client.TraceTransaction(
		context.Background(),
		"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Type != "CALL" || len(raw) == 0 {
		t.Fatalf("unexpected trace response: %+v %s", frame, raw)
	}
}
