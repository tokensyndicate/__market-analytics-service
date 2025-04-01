package models

// WSMessageType defines the type of WebSocket message
type WSMessageType string

const (
	// Command types
	WSTypeSubscribe   WSMessageType = "subscribe"
	WSTypeUnsubscribe WSMessageType = "unsubscribe"

	// Data types
	WSTypeCandles      WSMessageType = "candles"
	WSTypeOrderBook    WSMessageType = "orderbook"
	WSTypeOrderBookAgg WSMessageType = "orderbook_agg"
	WSTypeAlert        WSMessageType = "alert"
)

// WSMessage представляет базовую структуру WebSocket сообщения
type WSMessage struct {
	Type      WSMessageType `json:"type"`
	Timestamp string        `json:"timestamp"`
	Data      interface{}   `json:"data"`
	Error     string        `json:"error,omitempty"`
}

// ---- old code ----
// WSMetricsRequest represents a metrics subscription request
type WSMetricsRequest struct {
	Type        WSMessageType `json:"type"`
	Exchange    string        `json:"exchange"`
	TradingPair string        `json:"trading_pair"`
	Interval    string        `json:"interval,omitempty"` // e.g., "1s", "5s", "1m"
}

// WSMetricsResponse represents a metrics update message
type WSMetricsResponse struct {
	Type    WSMessageType  `json:"type"`
	Metrics *MarketMetrics `json:"metrics"`
	Error   string         `json:"error,omitempty"`
}
