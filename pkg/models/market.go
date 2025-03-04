package models

import "time"

// OrderBook represents a snapshot of market orders
type OrderBook struct {
	Exchange    string         `json:"exchange"`
	TradingPair string         `json:"trading_pair"`
	Timestamp   time.Time      `json:"timestamp"`
	Bids        []OrderBookRow `json:"bids"`
	Asks        []OrderBookRow `json:"asks"`
}

// OrderBookRow represents a single level in the order book
type OrderBookRow struct {
	Price       float64 `json:"price"`
	Volume      float64 `json:"volume"`
	TotalVolume float64 `json:"total_volume"`
}

// Trade represents a single trade execution
type Trade struct {
	Exchange      string    `json:"exchange"`
	TradingPair   string    `json:"trading_pair"`
	Timestamp     time.Time `json:"timestamp"`
	TradeID       string    `json:"trade_id"`
	OrderID       string    `json:"order_id"`
	Side          string    `json:"side"` // "buy" or "sell"
	Price         float64   `json:"price"`
	Volume        float64   `json:"volume"`
	Value         float64   `json:"value"`
	LiquidityRole string    `json:"liquidity_role"` // "maker" or "taker"
	FeeAmount     float64   `json:"fee_amount"`
	FeeCurrency   string    `json:"fee_currency"`
}

// MarketDataRequest represents parameters for historical data requests
type MarketDataRequest struct {
	Exchange    string    `json:"exchange"`
	TradingPair string    `json:"trading_pair"`
	StartTime   time.Time `json:"start_time"`
	EndTime     time.Time `json:"end_time"`
	Limit       int       `json:"limit,omitempty"`
}

// WSRequest represents a WebSocket subscription request
type WSRequest struct {
	Type        string   `json:"type"` // "subscribe" or "unsubscribe"
	Exchange    string   `json:"exchange"`
	TradingPair string   `json:"trading_pair"`
	Channels    []string `json:"channels"` // "orderbook", "trades"
}

// WSResponse represents a WebSocket response message
type WSResponse struct {
	Type      string      `json:"type"`      // "orderbook", "trade", "error"
	ClientID  string      `json:"client_id"` // From request header
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
	Error     string      `json:"error,omitempty"`
}

// ErrorResponse represents a standard error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// MarketData represents a generic market data message
type MarketData struct {
	Type        string      `json:"type"`
	Exchange    string      `json:"exchange"`
	TradingPair string      `json:"trading_pair"`
	Timestamp   time.Time   `json:"timestamp"`
	ClientID    string      `json:"client_id"`
	Data        interface{} `json:"data"`
}
