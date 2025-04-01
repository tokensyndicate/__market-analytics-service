package models

import "time"

// MarketData represents a generic market data message
type MarketData struct {
	Type        string      `json:"type"`
	Exchange    string      `json:"exchange"`
	TradingPair string      `json:"trading_pair"`
	Timestamp   time.Time   `json:"timestamp"`
	ClientID    string      `json:"client_id"`
	Data        interface{} `json:"data"`
}

// MarketMetrics represents key market indicators and statistics
type MarketMetrics struct {
	// Price metrics
	LastPrice    string `json:"last_price"`
	PriceChange  string `json:"price_change"` // 24h change in percentage
	HighPrice24h string `json:"high_price_24h"`
	LowPrice24h  string `json:"low_price_24h"`

	// Volume metrics
	Volume24h    string `json:"volume_24h"`
	VolumeChange string `json:"volume_change"` // 24h change in percentage

	// Order book metrics
	BidAskSpread string `json:"bid_ask_spread"`
	MarketDepth  string `json:"market_depth"` // Total volume in order book

	// Liquidity metrics
	Liquidity string `json:"liquidity"` // Available liquidity at ±2% from mid price

	// Trading activity
	TradeCount24h int `json:"trade_count_24h"`

	// Metadata
	Timestamp   time.Time `json:"timestamp"`
	Exchange    string    `json:"exchange"`
	TradingPair string    `json:"trading_pair"`
	ClientID    string    `json:"client_id"`
}

// Candle represents a single candlestick
type Candle struct {
	Exchange    string    `json:"exchange"`
	TradingPair string    `json:"trading_pair"`
	Interval    string    `json:"interval"`
	Timestamp   time.Time `json:"timestamp"`
	Open        string    `json:"open"`
	High        string    `json:"high"`
	Low         string    `json:"low"`
	Close       string    `json:"close"`
	Volume      string    `json:"volume"`
}

// OrderBookRow represents a single level in the order book
type OrderBookRow struct {
	Price  string `json:"price"`
	Volume string `json:"volume"`
}

// OrderBook represents the current state of the order book
type OrderBook struct {
	Exchange    string         `json:"exchange"`
	TradingPair string         `json:"trading_pair"`
	Timestamp   time.Time      `json:"timestamp"`
	Bids        []OrderBookRow `json:"bids"`
	Asks        []OrderBookRow `json:"asks"`
}

// Trade represents a single trade
type Trade struct {
	Exchange      string    `json:"exchange"`
	TradingPair   string    `json:"trading_pair"`
	Timestamp     time.Time `json:"timestamp"`
	TradeID       string    `json:"trade_id"`
	OrderID       string    `json:"order_id"`
	Side          string    `json:"side"`
	Price         string    `json:"price"`
	Volume        string    `json:"volume"`
	Value         string    `json:"value"`
	LiquidityRole string    `json:"liquidity_role"`
	FeeAmount     string    `json:"fee_amount"`
	FeeCurrency   string    `json:"fee_currency"`
}

type OrderBookAggregation struct {
	Exchange    string    `json:"exchange"`
	TradingPair string    `json:"trading_pair"`
	Timestamp   time.Time `json:"timestamp"`

	// Метрики цен
	MidPrice      string `json:"mid_price"`
	Spread        string `json:"spread"`
	SpreadPercent string `json:"spread_percent"`

	// Метрики объёма
	BidVolume       string `json:"bid_volume"`
	AskVolume       string `json:"ask_volume"`
	TotalVolume     string `json:"total_volume"`
	VolumeImbalance string `json:"volume_imbalance"`

	// Метрики ликвидности
	BidLiquidity string `json:"bid_liquidity"`
	AskLiquidity string `json:"ask_liquidity"`

	// Дополнительные метрики
	PriceLevel int    `json:"price_level"`
	UpdateID   string `json:"update_id"`
}
