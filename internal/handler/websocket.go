package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"market-analytics-service/internal/service"
	"market-analytics-service/pkg/metrics"
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
	upgrader  websocket.Upgrader
}

func NewWSHandler(analytics *service.AnalyticsService) *WSHandler {
	return &WSHandler{
		analytics: analytics,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // Configure appropriately in production
			},
		},
	}
}

// HandleWS handles WebSocket connections
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
	defer conn.Close()

	// Create context with cancellation
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Create subscription manager for this connection
	subscriptions := newSubscriptionManager(h.analytics, clientID)
	defer subscriptions.closeAll()

	// Start ping/pong
	go h.handlePing(ctx, conn)

	// Handle incoming messages
	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Error().Err(err).Msg("WebSocket read error")
			}
			return
		}

		if messageType != websocket.TextMessage {
			continue
		}

		if err := h.handleMessage(ctx, conn, message, subscriptions); err != nil {
			log.Error().Err(err).Msg("Failed to handle message")
			h.sendError(conn, err.Error())
		}
	}
}

// handleMessage processes incoming WebSocket messages
func (h *WSHandler) handleMessage(ctx context.Context, conn *websocket.Conn, message []byte, subs *subscriptionManager) error {
	var request models.WSMetricsRequest
	if err := json.Unmarshal(message, &request); err != nil {
		return fmt.Errorf("invalid message format: %w", err)
	}

	switch request.Type {
	case models.WSTypeSubscribe:
		return subs.subscribe(ctx, conn, request)
	case models.WSTypeUnsubscribe:
		return subs.unsubscribe(request)
	default:
		return fmt.Errorf("unknown message type: %s", request.Type)
	}
}

type subscriptionManager struct {
	analytics     *service.AnalyticsService
	clientID      string
	subscriptions map[string]context.CancelFunc
}

func newSubscriptionManager(analytics *service.AnalyticsService, clientID string) *subscriptionManager {
	return &subscriptionManager{
		analytics:     analytics,
		clientID:      clientID,
		subscriptions: make(map[string]context.CancelFunc),
	}
}

// subscribe creates a new metrics subscription
func (sm *subscriptionManager) subscribe(ctx context.Context, conn *websocket.Conn, request models.WSMetricsRequest) error {
	subKey := fmt.Sprintf("%s:%s", request.Exchange, request.TradingPair)

	// Cancel existing subscription if any
	if cancel, exists := sm.subscriptions[subKey]; exists {
		cancel()
	}

	// Create new context for this subscription
	subCtx, cancel := context.WithCancel(ctx)
	sm.subscriptions[subKey] = cancel

	// Parse update interval
	interval, err := time.ParseDuration(request.Interval)
	if err != nil {
		interval = time.Second // default interval
	}

	// Create subscription parameters
	params := metrics.SubscriptionParams{
		Exchange:     request.Exchange,
		TradingPair:  request.TradingPair,
		ClientID:     sm.clientID,
		UpdatePeriod: interval,
	}

	// Subscribe to metrics
	metricsChan, err := sm.analytics.SubscribeToMetrics(subCtx, params)
	if err != nil {
		return fmt.Errorf("failed to subscribe to metrics: %w", err)
	}

	// Start goroutine to handle metrics updates
	go func() {
		for {
			select {
			case <-subCtx.Done():
				return
			case metrics, ok := <-metricsChan:
				if !ok {
					return
				}

				response := models.WSMetricsResponse{
					Type:    models.WSTypeMetrics,
					Metrics: metrics, // Now this should work correctly
				}

				if err := conn.WriteJSON(response); err != nil {
					log.Error().Err(err).Msg("Failed to write metrics update")
					return
				}
			}
		}
	}()

	return nil
}

// unsubscribe cancels a metrics subscription
func (sm *subscriptionManager) unsubscribe(request models.WSMetricsRequest) error {
	subKey := fmt.Sprintf("%s:%s", request.Exchange, request.TradingPair)
	if cancel, exists := sm.subscriptions[subKey]; exists {
		cancel()
		delete(sm.subscriptions, subKey)
	}
	return nil
}

// closeAll cancels all active subscriptions
func (sm *subscriptionManager) closeAll() {
	for _, cancel := range sm.subscriptions {
		cancel()
	}
	sm.subscriptions = make(map[string]context.CancelFunc)
}

// handlePing maintains WebSocket connection
func (h *WSHandler) handlePing(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(time.Second)); err != nil {
				log.Error().Err(err).Msg("Failed to write ping message")
				return
			}
		}
	}
}

// sendError sends error message over WebSocket
func (h *WSHandler) sendError(conn *websocket.Conn, errorMsg string) {
	response := models.WSMetricsResponse{
		Type:  models.WSTypeError,
		Error: errorMsg,
	}

	if err := conn.WriteJSON(response); err != nil {
		log.Error().Err(err).Msg("Failed to send error message")
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
