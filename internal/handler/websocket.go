package handler

import (
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"

	"market-analytics-service/internal/ws"
)

type WSHandler struct {
	upgrader            websocket.Upgrader
	subscriptionManager *ws.SubscriptionManager
}

func NewWSHandler(subscriptionManager *ws.SubscriptionManager) *WSHandler {
	return &WSHandler{
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		subscriptionManager: subscriptionManager,
	}
}

func (h *WSHandler) HandleWS(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-User-ID")
	if clientID == "" {
		http.Error(w, "X-User-ID header is required", http.StatusUnauthorized)
		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to upgrade connection")
		return
	}

	log.Info().
		Str("clientID", clientID).
		Msg("WebSocket connection established")

	// Передаем subscriptionManager при создании Connection
	wsConn := ws.NewConnection(conn, clientID, h.subscriptionManager)
	h.subscriptionManager.AddConnection(wsConn)

	// Запускаем обработку соединения
	wsConn.Start()

	// Ждем закрытия соединения
	<-r.Context().Done()

	h.subscriptionManager.RemoveConnection(wsConn)
	wsConn.Close()
}
