package models

type CandleData [][]interface{} // [timestamp, open, high, low, close, volume]
type OrderBookData struct {
	Bids [][2]float64 `json:"bids"` // [price, volume]
	Asks [][2]float64 `json:"asks"` // [price, volume]
}

type OrderBookAggData struct {
	Time          string  `json:"time"`
	MidPrice      float64 `json:"midPrice"`
	Spread        float64 `json:"spread"`
	BidVolume     float64 `json:"bidVolume"`
	AskVolume     float64 `json:"askVolume"`
	VolumeBalance float64 `json:"volumeBalance"`
}

// CandleResponse представляет данные свечного графика
type CandleResponse struct {
	Categories []string           `json:"categories"` // Временные метки
	Series     []CandleSeriesData `json:"series"`     // Серии данных
}

type CandleSeriesData struct {
	Name string      `json:"name"`
	Type string      `json:"type"`
	Data [][]float64 `json:"data"` // [timestamp, open, close, low, high, volume]
}

// OrderBookResponse представляет данные стакана
type OrderBookResponse struct {
	Timestamp string       `json:"timestamp"`
	Bids      [][2]float64 `json:"bids"` // [price, volume]
	Asks      [][2]float64 `json:"asks"` // [price, volume]
}

// OrderBookAggResponse представляет агрегированные данные стакана
type OrderBookAggResponse struct {
	Timestamps []string   `json:"timestamps"`
	Metrics    AggMetrics `json:"metrics"`
}

type AggMetrics struct {
	MidPrice      []float64 `json:"midPrice"`
	Spread        []float64 `json:"spread"`
	BidVolume     []float64 `json:"bidVolume"`
	AskVolume     []float64 `json:"askVolume"`
	VolumeBalance []float64 `json:"volumeBalance"`
}
