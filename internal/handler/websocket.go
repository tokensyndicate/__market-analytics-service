package handler

import (
	"net/http"
	"time"

	"market-analytics-service/internal/service"
	"market-analytics-service/pkg/models"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // In production, implement proper origin checking
	},
}

type WSHandler struct {
	analytics *service.AnalyticsService
}

func NewWSHandler(analytics *service.AnalyticsService) *WSHandler {
	return &WSHandler{
		analytics: analytics,
	}
}

// HandleWS handles WebSocket connections
func (h *WSHandler) HandleWS(w http.ResponseWriter, r *http.Request) {
	// Extract client_id from header
	clientID := r.Header.Get("client_id")

	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "could not upgrade connection", http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	// Subscribe to market data
	dataCh, err := h.analytics.Subscribe(clientID)
	if err != nil {
		conn.WriteJSON(models.WSResponse{
			Type:  "error",
			Error: "failed to subscribe to market data",
		})
		return
	}
	defer h.analytics.Unsubscribe(clientID, dataCh)

	// Setup ping/pong
	conn.SetPingHandler(func(string) error {
		return conn.WriteControl(websocket.PongMessage, []byte{}, time.Now().Add(time.Second))
	})

	// Create done channel for cleanup
	done := make(chan struct{})
	defer close(done)

	// Start read pump to handle client messages (if needed)
	go func() {
		defer func() {
			done <- struct{}{}
		}()

		for {
			// Read message (required to handle client disconnection)
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// Main loop - write data to client
	for {
		select {
		case data, ok := <-dataCh:
			if !ok {
				return
			}
			if err := conn.WriteJSON(models.WSResponse{
				Type:      data.Type,
				ClientID:  data.ClientID,
				Timestamp: data.Timestamp,
				Data:      data.Data,
			}); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}
