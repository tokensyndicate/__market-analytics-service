package handler

import (
	"context"
	"net/http"
	"time"

	"market-analytics-service/internal/service"
	"market-analytics-service/pkg/models"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
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
	clientID := r.Header.Get("X-User-ID")

	log.Info().
		Str("clientID", clientID).
		Msg("WebSocket connection attempt")

	// Validate clientID
	if clientID == "" {
		http.Error(w, "X-User-ID header is required", http.StatusUnauthorized)
		return
	}

	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to upgrade connection")
		return
	}

	// Create context with cancellation for this connection
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	defer conn.Close()

	// Subscribe to market data
	dataCh, err := h.analytics.Subscribe(ctx, clientID)
	if err != nil {
		log.Error().Err(err).
			Str("clientID", clientID).
			Msg("Failed to subscribe to market data")

		conn.WriteJSON(models.WSResponse{
			Type:  "error",
			Error: "failed to subscribe to market data",
		})
		return
	}

	// Create done channel for cleanup
	done := make(chan struct{})

	// Start read pump to handle client disconnection
	go func() {
		defer close(done)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err,
					websocket.CloseGoingAway,
					websocket.CloseAbnormalClosure) {
					log.Debug().
						Err(err).
						Str("clientID", clientID).
						Msg("WebSocket read error")
				}
				return
			}
		}
	}()

	// Setup ping handler
	conn.SetPingHandler(func(string) error {
		return conn.WriteControl(
			websocket.PongMessage,
			[]byte{},
			time.Now().Add(time.Second),
		)
	})

	// Main loop - write data to client
	for {
		select {
		case data, ok := <-dataCh:
			if !ok {
				return
			}
			err := conn.WriteJSON(models.WSResponse{
				Type:      data.Type,
				ClientID:  data.ClientID,
				Timestamp: data.Timestamp,
				Data:      data.Data,
			})
			if err != nil {
				log.Error().
					Err(err).
					Str("clientID", clientID).
					Msg("Failed to write to WebSocket")
				return
			}
		case <-done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (h *WSHandler) getSubscriptionKey(userID, clientID string) string {
	if clientID != "" {
		return clientID
	}
	if userID != "" {
		return userID + ":all"
	}
	return "public"
}
