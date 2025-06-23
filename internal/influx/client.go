package influx

import (
	"context"
	"fmt"
	"market-analytics-service/pkg/models"
	"math"
	"sort"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/rs/zerolog/log"
)

// Client wraps InfluxDB client functionality
type Client struct {
	client   influxdb2.Client
	queryAPI api.QueryAPI // Исправленный тип
	org      string
	bucket   string
	buckets  Buckets
}

// Point represents an InfluxDB data point
type Point influxdb2.Point

type Buckets struct {
	Candles        string
	OrderBook      string
	OrderBookAgg   string
	MarketAnalysis string
}

// NewClient creates and initializes a new InfluxDB client
func NewClient(url, token, org string, buckets Buckets) (*Client, error) {
	client := influxdb2.NewClient(url, token)

	// Verify connection
	_, err := client.Ping(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to InfluxDB: %w", err)
	}

	return &Client{
		client:   client,
		queryAPI: client.QueryAPI(org),
		org:      org,
		buckets:  buckets,
	}, nil
}

func (c *Client) GetBucket(dataType string) string {
	switch dataType {
	case "candles":
		return c.buckets.Candles
	case "orderbook":
		return c.buckets.OrderBook
	case "orderbook_agg":
		return c.buckets.OrderBookAgg
	case "market_analysis":
		return c.buckets.MarketAnalysis
	default:
		return ""
	}
}

// GetQueryAPI returns the QueryAPI interface
func (c *Client) GetQueryAPI() api.QueryAPI {
	return c.queryAPI
}

// GetOrg returns the configured organization name
func (c *Client) GetOrg() string {
	return c.org
}

// Close releases the client resources
func (c *Client) Close() {
	c.client.Close()
}

// Client returns the underlying InfluxDB client
func (c *Client) Client() influxdb2.Client {
	return c.client
}

// NewPoint creates a new InfluxDB data point
func NewPoint(measurement string, tags map[string]string, fields map[string]interface{}, timestamp time.Time) Point {
	return influxdb2.NewPoint(measurement, tags, fields, timestamp)
}

// GetHistoricalData retrieves historical data for a specific time period
func (c *Client) GetHistoricalData(ctx context.Context, params QueryParams) ([]models.MarketData, error) {
	query := fmt.Sprintf(`
from(bucket:"%s")
    |> range(start: %s, stop: %s)
    |> filter(fn: (r) => r["_measurement"] == "candles")
    |> filter(fn: (r) => r["exchange"] == "%s")
    |> filter(fn: (r) => r["trading_pair"] == "%s")
    |> filter(fn: (r) => r["interval"] == "%s")
    |> filter(fn: (r) => r["_field"] =~ /^(open|high|low|close|volume)$/)
    |> keep(columns: ["_time", "_field", "_value", "exchange", "trading_pair", "interval"])
    |> group(columns: ["_field"])
    |> aggregateWindow(
        every: %s,
        fn: first,
        createEmpty: false
    )
    |> group(columns: ["exchange", "trading_pair", "interval", "_time"])
    |> pivot(
        rowKey: ["_time", "exchange", "trading_pair", "interval"],
        columnKey: ["_field"],
        valueColumn: "_value"
    )
    |> filter(fn: (r) =>
        exists r.open and
        exists r.high and
        exists r.low and
        exists r.close and
        exists r.volume
    )
`,
		params.Bucket,
		params.StartTime.Format(time.RFC3339),
		params.EndTime.Format(time.RFC3339),
		params.Exchange,
		params.TradingPair,
		params.Interval,
		params.Interval,
	)

	log.Debug().
		Str("query", query).
		Str("exchange", params.Exchange).
		Str("tradingPair", params.TradingPair).
		Str("interval", params.Interval).
		Time("startTime", params.StartTime).
		Time("endTime", params.EndTime).
		Msg("Executing InfluxDB query")

	result, err := c.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer result.Close()

	var data []models.MarketData
	for result.Next() {
		record := result.Record()

		values := make(map[string]interface{})
		for _, field := range []string{"open", "high", "low", "close", "volume"} {
			value := record.ValueByKey(field)
			if value == nil {
				log.Debug().
					Str("field", field).
					Time("timestamp", record.Time()).
					Msg("Missing field value")
				continue
			}
			values[field] = value
		}

		// Проверяем, что все необходимые поля присутствуют
		if len(values) != 5 {
			log.Debug().
				Time("timestamp", record.Time()).
				Int("fieldsCount", len(values)).
				Msg("Incomplete candle data")
			continue
		}

		marketData := models.MarketData{
			Type:        "candles",
			Exchange:    params.Exchange,    // Используем значения из параметров
			TradingPair: params.TradingPair, // так как они уже проверены в фильтрах
			Timestamp:   record.Time(),
			Data:        values,
		}

		data = append(data, marketData)
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error during query execution: %w", err)
	}

	log.Debug().
		Str("exchange", params.Exchange).
		Str("tradingPair", params.TradingPair).
		Int("dataPoints", len(data)).
		Msg("Historical data retrieved")

	return data, nil
}

// GetOrderBookData retrieves order book data for a specific time period
func (c *Client) GetOrderBookData(ctx context.Context, params QueryParams) ([]models.MarketData, error) {
	query := fmt.Sprintf(`
from(bucket:"%s")
    |> range(start: %s, stop: %s)
    |> filter(fn: (r) => r["_measurement"] == "orderbook")
    |> filter(fn: (r) => r["exchange"] == "%s")
    |> filter(fn: (r) => r["trading_pair"] == "%s")
    |> group(columns: ["side"])
    |> last()
    |> pivot(rowKey: ["_time", "side"], columnKey: ["_field"], valueColumn: "_value")
    |> filter(fn: (r) => exists r.price and exists r.volume)
    |> filter(fn: (r) => r.volume > 0)
`,
		params.Bucket,
		params.StartTime.Format(time.RFC3339),
		params.EndTime.Format(time.RFC3339),
		params.Exchange,
		params.TradingPair,
	)

	log.Debug().
		Str("query", query).
		Str("bucket", params.Bucket).
		Str("exchange", params.Exchange).
		Str("tradingPair", params.TradingPair).
		Time("startTime", params.StartTime).
		Time("endTime", params.EndTime).
		Msg("Executing order book query")

	result, err := c.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer result.Close()

	var data []models.MarketData

	for result.Next() {
		record := result.Record()

		log.Debug().
			Interface("values", record.Values()).
			Msg("Processing record")

		side := record.ValueByKey("side").(string)
		price := record.ValueByKey("price").(float64)
		volume := record.ValueByKey("volume").(float64)

		marketData := models.MarketData{
			Type:        "orderbook",
			Exchange:    params.Exchange,
			TradingPair: params.TradingPair,
			Timestamp:   record.Time(),
			Data: map[string]interface{}{
				"side":   side,
				"price":  price,
				"volume": volume,
			},
		}

		data = append(data, marketData)

		log.Debug().
			Str("side", side).
			Float64("price", price).
			Float64("volume", volume).
			Time("timestamp", record.Time()).
			Msg("Added order book entry")
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error during query execution: %w", err)
	}

	log.Info().
		Int("recordCount", len(data)).
		Str("exchange", params.Exchange).
		Str("tradingPair", params.TradingPair).
		Msg("Retrieved order book data")

	return data, nil
}

// SubscribeToData subscribes to real-time market data updates
func (c *Client) SubscribeToData(ctx context.Context, query string) (<-chan models.MarketData, error) {
	dataCh := make(chan models.MarketData, 100)

	// Construct the complete Flux query with correct syntax
	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: -1s)
`, c.bucket)

	log.Debug().
		Str("query", fluxQuery).
		Msg("Creating InfluxDB subscription")

	go func() {
		defer close(dataCh)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				result, err := c.queryAPI.Query(ctx, fluxQuery)
				if err != nil {
					log.Error().Err(err).Msg("InfluxDB query error")
					time.Sleep(time.Second)
					continue
				}

				hasData := false
				for result.Next() {
					hasData = true
					record := result.Record()

					data := models.MarketData{
						Type:        record.Measurement(),
						Exchange:    record.ValueByKey("exchange").(string),
						TradingPair: record.ValueByKey("trading_pair").(string),
						Timestamp:   record.Time(),
						ClientID:    record.ValueByKey("client_id").(string),
						Data:        record.Values(),
					}

					select {
					case dataCh <- data:
					case <-ctx.Done():
						result.Close()
						return
					}
				}
				result.Close()

				if !hasData {
					time.Sleep(time.Millisecond * 100)
				}
			}
		}
	}()

	return dataCh, nil
}

func (c *Client) GetLatestOrderBook(ctx context.Context, exchange, tradingPair string) (*models.OrderBook, error) {
	// Получаем данные за последние 1 секунду для самых свежих данных
	query := fmt.Sprintf(`
from(bucket: "%s")
    |> range(start: -1s)
    |> filter(fn: (r) => r["_measurement"] == "orderbook")
    |> filter(fn: (r) => r["exchange"] == "%s")
    |> filter(fn: (r) => r["trading_pair"] == "%s")
    |> last()
`, c.buckets.OrderBook, exchange, tradingPair)

	log.Info().
		Str("bucket", c.buckets.OrderBook).
		Str("exchange", exchange).
		Str("tradingPair", tradingPair).
		Str("query", query).
		Msg("Debug InfluxDB query")

	result, err := c.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer result.Close()

	ob := &models.OrderBook{
		Exchange:    exchange,
		TradingPair: tradingPair,
		Timestamp:   time.Now(),
		Bids:        make([]models.OrderBookRow, 0),
		Asks:        make([]models.OrderBookRow, 0),
	}

	bids := make(map[float64]struct {
		Volume      float64
		TotalVolume float64
		Level       int
	})

	asks := make(map[float64]struct {
		Volume      float64
		TotalVolume float64
		Level       int
	})

	var lastTimestamp time.Time

	for result.Next() {
		record := result.Record()

		// Обновляем временную метку
		recordTime := record.Time()
		if recordTime.After(lastTimestamp) {
			lastTimestamp = recordTime
			ob.Timestamp = recordTime
		}

		side := record.ValueByKey("side").(string)
		price := record.ValueByKey("price").(float64)
		volume := record.ValueByKey("volume").(float64)
		level := record.ValueByKey("level").(int)

		// Получаем аккумулированный объем, если доступен
		totalVolume, ok := record.ValueByKey("total_volume").(float64)
		if !ok {
			totalVolume = 0 // Если поле отсутствует или нулевое
		}

		if volume <= 0 {
			continue
		}

		switch side {
		case "bid":
			bids[price] = struct {
				Volume      float64
				TotalVolume float64
				Level       int
			}{
				Volume:      volume,
				TotalVolume: totalVolume,
				Level:       level,
			}
		case "ask":
			asks[price] = struct {
				Volume      float64
				TotalVolume float64
				Level       int
			}{
				Volume:      volume,
				TotalVolume: totalVolume,
				Level:       level,
			}
		}
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error during query execution: %w", err)
	}

	// Проверка на пустой ордербук
	if len(bids) == 0 && len(asks) == 0 {
		return nil, fmt.Errorf("empty orderbook for %s on %s", tradingPair, exchange)
	}

	// Проверка на отрицательный спред
	var highestBid float64
	var lowestAsk float64 = math.MaxFloat64

	for price := range bids {
		if price > highestBid {
			highestBid = price
		}
	}

	for price := range asks {
		if price < lowestAsk {
			lowestAsk = price
		}
	}

	// Если спред отрицательный, удаляем конфликтующие ордера
	if highestBid >= lowestAsk && highestBid > 0 && lowestAsk < math.MaxFloat64 {
		log.Warn().
			Float64("highestBid", highestBid).
			Float64("lowestAsk", lowestAsk).
			Str("exchange", exchange).
			Str("tradingPair", tradingPair).
			Msg("Negative spread detected, cleaning order book")

		for price := range bids {
			if price >= lowestAsk {
				delete(bids, price)
			}
		}

		for price := range asks {
			if price <= highestBid {
				delete(asks, price)
			}
		}
	}

	// Конвертируем карты в отсортированные слайсы, сортируя по уровню
	type PriceLevel struct {
		Price       float64
		Volume      float64
		TotalVolume float64
		Level       int
	}

	bidLevels := make([]PriceLevel, 0, len(bids))
	for price, data := range bids {
		bidLevels = append(bidLevels, PriceLevel{
			Price:       price,
			Volume:      data.Volume,
			TotalVolume: data.TotalVolume,
			Level:       data.Level,
		})
	}
	// Сортируем по уровню, чтобы сохранить порядок из ордербука
	sort.Slice(bidLevels, func(i, j int) bool {
		return bidLevels[i].Level < bidLevels[j].Level
	})

	askLevels := make([]PriceLevel, 0, len(asks))
	for price, data := range asks {
		askLevels = append(askLevels, PriceLevel{
			Price:       price,
			Volume:      data.Volume,
			TotalVolume: data.TotalVolume,
			Level:       data.Level,
		})
	}
	// Сортируем по уровню, чтобы сохранить порядок из ордербука
	sort.Slice(askLevels, func(i, j int) bool {
		return askLevels[i].Level < askLevels[j].Level
	})

	// Заполняем результат, используя предрассчитанные аккумулированные объемы
	// если они доступны, иначе рассчитываем их на лету

	// Для bids
	var accumulatedBidVolume float64
	for _, level := range bidLevels {
		if level.TotalVolume > 0 {
			// Используем предрассчитанный аккумулированный объем
			accumulatedBidVolume = level.TotalVolume
		} else {
			// Рассчитываем на лету (backwards compatibility)
			accumulatedBidVolume += level.Volume
		}

		ob.Bids = append(ob.Bids, models.OrderBookRow{
			Price:       fmt.Sprintf("%.8f", level.Price),
			Volume:      fmt.Sprintf("%.8f", level.Volume),
			TotalVolume: fmt.Sprintf("%.8f", accumulatedBidVolume),
		})
	}

	// Для asks
	var accumulatedAskVolume float64
	for _, level := range askLevels {
		if level.TotalVolume > 0 {
			// Используем предрассчитанный аккумулированный объем
			accumulatedAskVolume = level.TotalVolume
		} else {
			// Рассчитываем на лету (backwards compatibility)
			accumulatedAskVolume += level.Volume
		}

		ob.Asks = append(ob.Asks, models.OrderBookRow{
			Price:       fmt.Sprintf("%.8f", level.Price),
			Volume:      fmt.Sprintf("%.8f", level.Volume),
			TotalVolume: fmt.Sprintf("%.8f", accumulatedAskVolume),
		})
	}

	log.Debug().
		Str("exchange", exchange).
		Str("tradingPair", tradingPair).
		Int("bids", len(ob.Bids)).
		Int("asks", len(ob.Asks)).
		Time("timestamp", ob.Timestamp).
		Msg("Order book retrieved successfully with both original and accumulated volumes")

	return ob, nil
}

// QueryParams represents parameters for historical data queries
type QueryParams struct {
	ClientID    string
	StartTime   time.Time
	EndTime     time.Time
	Exchange    string
	TradingPair string
	Bucket      string
	Interval    string
}

// GetOrderBookAggData retrieves aggregated order book data for a specific time period
func (c *Client) GetOrderBookAggData(ctx context.Context, params QueryParams) ([]models.MarketData, error) {
	// Убедимся, что интервал не пустой
	interval := params.Interval
	if interval == "" {
		interval = "1m" // Значение по умолчанию
	}

	query := fmt.Sprintf(`
from(bucket:"%s")
    |> range(start: %s, stop: %s)
    |> filter(fn: (r) => r["_measurement"] == "orderbook_aggregations")
    |> filter(fn: (r) => r["exchange"] == "%s")
    |> filter(fn: (r) => r["trading_pair"] == "%s")
    |> filter(fn: (r) => r["interval"] == "%s")
    |> pivot(rowKey: ["_time", "exchange", "trading_pair", "interval"],
             columnKey: ["_field"],
             valueColumn: "_value")
    |> drop(columns: ["_start", "_stop", "result", "table"])
`,
		params.Bucket,
		params.StartTime.Format(time.RFC3339),
		params.EndTime.Format(time.RFC3339),
		params.Exchange,
		params.TradingPair,
		interval)

	log.Debug().
		Time("startTime", params.StartTime).
		Time("endTime", params.EndTime).
		Str("exchange", params.Exchange).
		Str("tradingPair", params.TradingPair).
		Str("interval", interval).
		Str("query", query).
		Msg("Executing InfluxDB query")

	// Выполняем запрос
	result, err := c.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer result.Close()

	var data []models.MarketData
	for result.Next() {
		record := result.Record()

		// Создаем объект с общими полями
		marketData := models.MarketData{
			Type:        "orderbook_agg",
			Exchange:    params.Exchange,
			TradingPair: params.TradingPair,
			Timestamp:   record.Time(),
			Data:        make(map[string]interface{}),
		}

		// Добавляем все поля из записи в data
		// Ожидаемые поля: ask_depth, average_spread, bid_depth, buy_pressure, depth_imbalance,
		// max_spread, mid_price, min_spread, sell_pressure, snapshot_count, spread_volatility
		fields := []string{
			"ask_depth", "average_spread", "bid_depth", "buy_pressure", "depth_imbalance",
			"max_spread", "mid_price", "min_spread", "sell_pressure", "snapshot_count", "spread_volatility",
		}

		dataMap := marketData.Data.(map[string]interface{})
		for _, field := range fields {
			if val, ok := record.ValueByKey(field).(float64); ok {
				dataMap[field] = val
			}
		}

		// Также добавим информацию об интервале
		dataMap["interval"] = interval

		data = append(data, marketData)
	}

	// Проверяем ошибки после обработки результатов
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error processing query results: %w", err)
	}

	log.Debug().
		Int("dataPoints", len(data)).
		Msg("Order book aggregation data fetched")

	return data, nil
}
