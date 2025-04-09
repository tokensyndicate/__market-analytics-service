package service

import (
	"market-analytics-service/pkg/models"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
)

type DataTransformer struct{}

func NewDataTransformer() *DataTransformer {
	return &DataTransformer{}
}

// TransformCandles преобразует сырые данные в формат свечного графика
func (t *DataTransformer) TransformCandles(rawData []models.MarketData) [][]interface{} {
	result := make([][]interface{}, 0, len(rawData))

	for _, data := range rawData {
		timestamp := data.Timestamp.UnixMilli()
		if values, ok := data.Data.(map[string]interface{}); ok {
			// Безопасное извлечение значений
			open := safeGetFloat(values["open"])
			high := safeGetFloat(values["high"])
			low := safeGetFloat(values["low"])
			close := safeGetFloat(values["close"])
			volume := safeGetFloat(values["volume"])

			// Проверяем валидность данных
			if open == 0 && high == 0 && low == 0 && close == 0 && volume == 0 {
				continue
			}

			candle := []interface{}{
				timestamp,
				open,
				high,
				low,
				close,
				volume,
			}
			result = append(result, candle)
		}
	}

	// Сортируем по времени
	sort.Slice(result, func(i, j int) bool {
		return result[i][0].(int64) < result[j][0].(int64)
	})

	return result
}

func safeGetFloat(v interface{}) float64 {
	if v == nil {
		return 0
	}

	switch value := v.(type) {
	case float64:
		return value
	case int64:
		return float64(value)
	case int:
		return float64(value)
	case string:
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
	}
	return 0
}

// TransformOrderBook преобразует сырые данные в формат стакана
func (t *DataTransformer) TransformOrderBook(rawData []models.MarketData) models.OrderBookData {
	result := models.OrderBookData{
		Bids: make([][2]float64, 0),
		Asks: make([][2]float64, 0),
	}

	log.Debug().
		Int("rawDataLength", len(rawData)).
		Msg("Starting order book transformation")

	// Временные карты для группировки объемов по ценам
	bidMap := make(map[float64]float64)
	askMap := make(map[float64]float64)

	for _, data := range rawData {
		if values, ok := data.Data.(map[string]interface{}); ok {
			side, ok1 := values["side"].(string)
			price, ok2 := values["price"].(float64)
			volume, ok3 := values["volume"].(float64)

			if !ok1 || !ok2 || !ok3 {
				log.Debug().
					Interface("values", values).
					Msg("Invalid order book entry data types")
				continue
			}

			log.Debug().
				Str("side", side).
				Float64("price", price).
				Float64("volume", volume).
				Msg("Processing order book entry")

			if volume > 0 {
				if side == "bid" {
					bidMap[price] += volume
				} else if side == "ask" {
					askMap[price] += volume
				}
			}
		}
	}

	// Проверка на отрицательный спред
	var highestBid float64
	var lowestAsk float64 = math.MaxFloat64

	for price := range bidMap {
		if price > highestBid {
			highestBid = price
		}
	}

	for price := range askMap {
		if price < lowestAsk {
			lowestAsk = price
		}
	}

	// Если обнаружен отрицательный спред, исправляем его
	if highestBid >= lowestAsk && highestBid > 0 && lowestAsk < math.MaxFloat64 {
		log.Warn().
			Float64("highestBid", highestBid).
			Float64("lowestAsk", lowestAsk).
			Msg("Negative spread detected, cleaning order book")

		for price := range bidMap {
			if price >= lowestAsk {
				delete(bidMap, price)
			}
		}

		for price := range askMap {
			if price <= highestBid {
				delete(askMap, price)
			}
		}
	}

	// Конвертируем карты в слайсы для сортировки
	bidPrices := make([]float64, 0, len(bidMap))
	askPrices := make([]float64, 0, len(askMap))

	for price := range bidMap {
		bidPrices = append(bidPrices, price)
	}

	for price := range askMap {
		askPrices = append(askPrices, price)
	}

	// Сортируем цены
	sort.Slice(bidPrices, func(i, j int) bool {
		return bidPrices[i] > bidPrices[j] // По убыванию для бидов
	})
	sort.Slice(askPrices, func(i, j int) bool {
		return askPrices[i] < askPrices[j] // По возрастанию для асков
	})

	// Аккумулируем объемы
	var accumulatedBidVolume float64
	for _, price := range bidPrices {
		accumulatedBidVolume += bidMap[price]
		result.Bids = append(result.Bids, [2]float64{price, accumulatedBidVolume})
	}

	var accumulatedAskVolume float64
	for _, price := range askPrices {
		accumulatedAskVolume += askMap[price]
		result.Asks = append(result.Asks, [2]float64{price, accumulatedAskVolume})
	}

	// Проверяем, что объемы строго возрастают
	for i := 1; i < len(result.Bids); i++ {
		if result.Bids[i][1] < result.Bids[i-1][1] {
			result.Bids[i][1] = result.Bids[i-1][1]
		}
	}

	for i := 1; i < len(result.Asks); i++ {
		if result.Asks[i][1] < result.Asks[i-1][1] {
			result.Asks[i][1] = result.Asks[i-1][1]
		}
	}

	log.Info().
		Int("bidsCount", len(result.Bids)).
		Int("asksCount", len(result.Asks)).
		Msg("Order book transformation completed with accumulated volumes")

	return result
}

func (t *DataTransformer) TransformOrderBookAgg(data []models.MarketData) interface{} {
	if len(data) == 0 {
		return nil
	}

	// Находим самую последнюю запись
	var latestData models.MarketData
	latestTime := time.Time{}

	for _, item := range data {
		if item.Timestamp.After(latestTime) {
			latestTime = item.Timestamp
			latestData = item
		}
	}

	// Проверяем, что у нас есть данные Map
	dataMap, ok := latestData.Data.(map[string]interface{})
	if !ok {
		log.Warn().
			Interface("data", latestData.Data).
			Msg("Failed to convert orderbook_agg data to map")
		return nil
	}

	// Создаем копию карты, чтобы не модифицировать исходные данные
	result := make(map[string]interface{})
	for k, v := range dataMap {
		result[k] = v
	}

	// Добавляем метаданные
	result["timestamp"] = latestData.Timestamp.Format(time.RFC3339)
	result["exchange"] = latestData.Exchange
	result["trading_pair"] = latestData.TradingPair

	return result
}

// ValidateAndCleanData проверяет и очищает данные
func (t *DataTransformer) ValidateAndCleanData(data interface{}) interface{} {
	switch v := data.(type) {
	case float64:
		if v == 0 || v != v { // Проверка на NaN
			return nil
		}
		return v
	case string:
		if v == "" {
			return nil
		}
		return v
	default:
		return nil
	}
}
