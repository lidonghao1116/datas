package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	websocketWriteWait  = 10 * time.Second
	websocketPongWait   = 60 * time.Second
	websocketPingPeriod = 45 * time.Second
	websocketMaxMessage = 1024
	websocketQueueSize  = 64
)

type Hub struct {
	mu      sync.RWMutex
	clients map[*websocketClient]struct{}
	logger  *slog.Logger
}

func NewHub(logger *slog.Logger) *Hub {
	return &Hub{
		clients: make(map[*websocketClient]struct{}),
		logger:  logger,
	}
}

func (h *Hub) BroadcastAlert(alert Alert) {
	message, err := json.Marshal(map[string]any{
		"type": "alert",
		"data": alert,
	})
	if err != nil {
		h.logger.Error("encode WebSocket alert", "error", err)
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for client := range h.clients {
		select {
		case client.send <- message:
		default:
			go client.close()
		}
	}
}

func (h *Hub) register(client *websocketClient) {
	h.mu.Lock()
	h.clients[client] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) unregister(client *websocketClient) {
	h.mu.Lock()
	delete(h.clients, client)
	h.mu.Unlock()
}

func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

type websocketClient struct {
	hub       *Hub
	conn      *websocket.Conn
	send      chan []byte
	closeOnce sync.Once
}

func (c *websocketClient) close() {
	c.closeOnce.Do(func() {
		c.hub.unregister(c)
		close(c.send)
		c.conn.Close()
	})
}

func (c *websocketClient) readPump() {
	defer c.close()
	c.conn.SetReadLimit(websocketMaxMessage)
	c.conn.SetReadDeadline(time.Now().Add(websocketPongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(websocketPongWait))
		return nil
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (c *websocketClient) writePump(ctx context.Context) {
	ticker := time.NewTicker(websocketPingPeriod)
	defer ticker.Stop()
	defer c.close()
	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(websocketWriteWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(websocketWriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func newUpgrader(originAllowed func(*http.Request) bool) websocket.Upgrader {
	return websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 4096,
		CheckOrigin:     originAllowed,
	}
}
