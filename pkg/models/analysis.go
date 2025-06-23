package models

import "time"

// MarketMakerPattern represents identified market maker patterns
type MarketMakerPattern struct {
	Exchange    string    `json:"exchange"`
	TradingPair string    `json:"trading_pair"`
	Timestamp   time.Time `json:"timestamp"`
	ClientID    string    `json:"client_id"`
	PatternType string    `json:"pattern_type"`
	Confidence  float64   `json:"confidence"` // 0.0-1.0 scale
	Parameters  map[string]interface{} `json:"parameters"`
}

// PatternType definitions
const (
	PatternSpoofing       = "spoofing"
	PatternLayering       = "layering"
	PatternMomentumIgnition = "momentum_ignition"
	PatternIceberg        = "iceberg_orders"
	PatternPumpAndDump    = "pump_and_dump"
	PatternWashTrading    = "wash_trading"
	PatternStop           = "stop_hunting"
	PatternTime           = "time_weighted"
	PatternTwap           = "twap"
	PatternVwap           = "vwap"
)

// MMAnalysisConfig contains configuration for market maker analysis
type MMAnalysisConfig struct {
	MinOrderBookDepth   int     `json:"min_orderbook_depth"`
	MinCandleCount      int     `json:"min_candle_count"`
	ConfidenceThreshold float64 `json:"confidence_threshold"`
	TimeWindowMinutes   int     `json:"time_window_minutes"`
}

// MMAnalysisResult contains the results of a market maker analysis
type MMAnalysisResult struct {
	Exchange       string                `json:"exchange"`
	TradingPair    string                `json:"trading_pair"` 
	AnalysisTime   time.Time             `json:"analysis_time"`
	WindowStart    time.Time             `json:"window_start"`
	WindowEnd      time.Time             `json:"window_end"`
	DetectedPatterns []*MarketMakerPattern `json:"detected_patterns"`
	MetricsSnapshot map[string]float64   `json:"metrics_snapshot"`
}

// OrderLevel represents a specific price level in the order book
type OrderLevel struct {
	Price     float64
	Volume    float64
	Side      string // "bid" or "ask"
	Timestamp time.Time
	Duration  time.Duration // How long the order has been in the book
}

// VolumeProfile represents volume distribution across price ranges
type VolumeProfile struct {
	PriceStart    float64
	PriceEnd      float64
	TotalVolume   float64
	BidVolume     float64
	AskVolume     float64
	TradeCount    int
	VolumeWeightedPrice float64
}