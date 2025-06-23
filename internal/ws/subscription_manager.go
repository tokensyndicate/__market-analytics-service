package ws

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"market-analytics-service/internal/cache"
	"market-analytics-service/internal/influx"
	"market-analytics-service/internal/service"
	"market-analytics-service/pkg/models"
)

type SubscriptionManager struct {
	influxClient  *influx.Client
	cache         *cache.Store
	connections   map[*Connection]struct{}
	subscriptions map[string]map[*Connection][]models.Subscription
	mu            sync.RWMutex
}

func NewSubscriptionManager(influxClient *influx.Client) *SubscriptionManager {
	sm := &SubscriptionManager{
		influxClient:  influxClient,
		cache:         cache.NewStore(5 * time.Second),
		connections:   make(map[*Connection]struct{}),
		subscriptions: make(map[string]map[*Connection][]models.Subscription),
	}

	// Проверяем подключение к InfluxDB
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Пробуем выполнить простой запрос
	query := fmt.Sprintf(`from(bucket:"%s") |> range(start: -1m) |> limit(n:1)`,
		influxClient.GetBucket("candles"))

	result, err := influxClient.GetQueryAPI().Query(ctx, query)
	if err != nil {
		log.Error().Err(err).Msg("Failed to connect to InfluxDB")
	} else {
		result.Close()
		log.Info().Msg("Successfully connected to InfluxDB")
	}

	return sm
}

func (sm *SubscriptionManager) addSubscription(conn *Connection, sub models.Subscription) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	key := fmt.Sprintf("%s:%s:%s", sub.Exchange, sub.TradingPair, string(sub.Type))

	if sm.subscriptions[key] == nil {
		sm.subscriptions[key] = make(map[*Connection][]models.Subscription)
	}

	sm.subscriptions[key][conn] = append(sm.subscriptions[key][conn], sub)
}

func (sm *SubscriptionManager) removeSubscription(conn *Connection, sub models.Subscription) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	key := fmt.Sprintf("%s:%s:%s", sub.Exchange, sub.TradingPair, string(sub.Type))

	if subs, exists := sm.subscriptions[key]; exists {
		delete(subs, conn)
		if len(subs) == 0 {
			delete(sm.subscriptions, key)
		}
	}
}

func (sm *SubscriptionManager) AddConnection(conn *Connection) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.connections[conn] = struct{}{}
}

func (sm *SubscriptionManager) handleSubscription(ctx context.Context, conn *Connection, sub models.Subscription) error {
	// Добавляем подписку в менеджер
	sm.addSubscription(conn, sub)

	// Создаем отдельный контекст для этой подписки
	subCtx, cancel := context.WithCancel(ctx)
	conn.AddCancelFunc(sub, cancel)

	switch sub.Type {
	case models.SubTypeCandles:
		go sm.streamCandles(subCtx, conn, sub)
	case models.SubTypeOrderBook:
		go sm.streamOrderBook(subCtx, conn, sub)
	case models.SubTypeOrderBookAgg:
		go sm.streamOrderBookAgg(subCtx, conn, sub)
	}

	return nil
}

func (sm *SubscriptionManager) streamCandles(ctx context.Context, conn *Connection, sub models.Subscription) {
	log.Info().
		Str("clientID", conn.GetClientID()).
		Str("exchange", sub.Exchange).
		Str("tradingPair", sub.TradingPair).
		Str("interval", sub.Interval).
		Str("window", sub.Window).
		Msg("Starting candles stream")

	// Получаем интервал и окно из подписки
	interval, err := parseInterval(sub.Interval)
	if err != nil {
		log.Error().Err(err).Str("interval", sub.Interval).Msg("Invalid interval")
		return
	}

	window, err := parseWindow(sub.Window, sub.Interval)
	if err != nil {
		log.Error().Err(err).
			Str("window", sub.Window).
			Str("interval", sub.Interval).
			Msg("Invalid window, using default")

		window, err = parseWindow(getDefaultWindow(sub.Interval), sub.Interval)
		if err != nil {
			log.Error().Err(err).Msg("Failed to get default window")
			return
		}
	}

	if window < interval {
		log.Error().
			Str("window", sub.Window).
			Str("interval", sub.Interval).
			Msg("Window must be greater than interval")
		return
	}

	log.Info().
		Str("exchange", sub.Exchange).
		Str("tradingPair", sub.TradingPair).
		Str("interval", sub.Interval).
		Dur("window", window).
		Msg("Starting candle stream with window")

	// Используем интервал свечи для тикера
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Функция для получения и отправки данных
	sendCandleData := func() {
		now := time.Now()

		params := influx.QueryParams{
			StartTime:   now.Add(-window),
			EndTime:     now,
			ClientID:    conn.GetClientID(),
			Exchange:    sub.Exchange,
			TradingPair: sub.TradingPair,
			Bucket:      sm.influxClient.GetBucket("candles"),
			Interval:    sub.Interval,
		}

		data, err := sm.influxClient.GetHistoricalData(ctx, params)
		if err != nil {
			log.Error().
				Err(err).
				Str("exchange", sub.Exchange).
				Str("tradingPair", sub.TradingPair).
				Msg("Failed to get candles")
			return
		}

		if len(data) > 0 {
			transformer := service.NewDataTransformer()
			candleData := transformer.TransformCandles(data)

			msg := models.WSMessage{
				Type:        models.WSTypeCandles,
				Exchange:    sub.Exchange,
				TradingPair: sub.TradingPair,
				Interval:    sub.Interval,
				Timestamp:   now.Format(time.RFC3339),
				Data:        candleData,
			}

			if err := conn.SendMessage(msg); err != nil {
				log.Error().
					Err(err).
					Str("exchange", sub.Exchange).
					Str("tradingPair", sub.TradingPair).
					Msg("Failed to send candles")
				return
			}

			log.Debug().
				Str("exchange", sub.Exchange).
				Str("tradingPair", sub.TradingPair).
				Int("count", len(candleData)).
				Time("windowStart", params.StartTime).
				Time("windowEnd", params.EndTime).
				Msg("Sent candle window")
		}
	}

	// Отправляем начальные данные
	sendCandleData()

	// Ждем следующего интервала для первого обновления
	nextInterval := time.Now().Truncate(interval).Add(interval)
	time.Sleep(time.Until(nextInterval))

	// Сбрасываем тикер, чтобы он начал точно с нового интервала
	ticker.Reset(interval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sendCandleData()
		}
	}
}

func parseInterval(interval string) (time.Duration, error) {
	switch interval {
	case "1m":
		return time.Minute, nil
	case "5m":
		return 5 * time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("invalid interval: %s", interval)
	}
}

func getDefaultWindow(interval string) string {
	switch interval {
	case "1m":
		return "1d" // 1440 свечей
	case "5m":
		return "1d" // 288 свечей
	case "15m":
		return "3d" // 288 свечей
	case "1h":
		return "7d" // 168 свечей
	case "1d":
		return "30d" // 30 свечей
	default:
		return "1d"
	}
}

func parseWindow(window, interval string) (time.Duration, error) {
	// Если окно не указано, используем значение по умолчанию
	if window == "" {
		window = getDefaultWindow(interval)
	}

	switch window {
	case "1h":
		return time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	case "12h":
		return 12 * time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	case "3d":
		return 3 * 24 * time.Hour, nil
	case "7d":
		return 7 * 24 * time.Hour, nil
	case "30d":
		return 30 * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("invalid window: %s", window)
	}
}

func (sm *SubscriptionManager) streamOrderBook(ctx context.Context, conn *Connection, sub models.Subscription) {
	log.Info().
		Str("clientID", conn.GetClientID()).
		Str("exchange", sub.Exchange).
		Str("tradingPair", sub.TradingPair).
		Msg("Starting order book stream")

	// Создаем контекст с отменой
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Создаем каналы для корректного завершения всех горутин
	done := make(chan struct{})
	defer close(done)

	// Отдельная горутина для мониторинга состояния соединения
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if conn.IsClosed() {
					cancel()
					return
				}
			}
		}
	}()

	// Структуры для хранения состояния стакана
	var orderBookState = map[string]map[float64]float64{
		"bid": make(map[float64]float64),
		"ask": make(map[float64]float64),
	}

	// Ticker для запросов данных и обновлений
	dataQueryTicker := time.NewTicker(1 * time.Second)
	defer dataQueryTicker.Stop()

	for {
		select {
		case <-streamCtx.Done():
			log.Info().
				Str("clientID", conn.GetClientID()).
				Str("exchange", sub.Exchange).
				Str("tradingPair", sub.TradingPair).
				Msg("Stopping order book stream")
			return

		case <-dataQueryTicker.C:
			if conn.IsClosed() {
				log.Info().
					Str("clientID", conn.GetClientID()).
					Msg("Connection closed, stopping order book stream")
				return
			}

			// Очищаем предыдущее состояние
			orderBookState = map[string]map[float64]float64{
				"bid": make(map[float64]float64),
				"ask": make(map[float64]float64),
			}

			// Запрос к InfluxDB
			query := fmt.Sprintf(`
from(bucket:"%s")
    |> range(start: -1s)
    |> filter(fn: (r) => r["_measurement"] == "orderbook")
    |> filter(fn: (r) => r["exchange"] == "%s")
    |> filter(fn: (r) => r["trading_pair"] == "%s")
    |> pivot(rowKey:["_time", "_start", "_stop", "exchange", "trading_pair", "side"], columnKey: ["_field"], valueColumn: "_value")
`,
				"trading_orderbook",
				sub.Exchange,
				sub.TradingPair,
			)

			result, err := sm.influxClient.GetQueryAPI().Query(streamCtx, query)
			if err != nil {
				log.Error().
					Err(err).
					Str("clientID", conn.GetClientID()).
					Msg("Failed to execute order book query")
				continue
			}

			// Обрабатываем результаты
			func() {
				defer result.Close()

				for result.Next() {
					if conn.IsClosed() || streamCtx.Err() != nil {
						return
					}

					record := result.Record()
					side := record.ValueByKey("side").(string)
					price := record.ValueByKey("price").(float64)
					volume := record.ValueByKey("volume").(float64)

					// Фильтруем нулевые объемы
					if volume > 0 {
						orderBookState[side][price] = volume
					}
				}

				if err := result.Err(); err != nil {
					log.Error().
						Err(err).
						Str("clientID", conn.GetClientID()).
						Msg("Error in order book query results")
					return
				}
			}()

			// Пропускаем отправку, если книга пуста
			if len(orderBookState["bid"]) == 0 && len(orderBookState["ask"]) == 0 {
				log.Debug().Msg("Order book is empty, skipping update")
				continue
			}

			// Формируем данные для отправки
			bids := make([][]float64, 0, len(orderBookState["bid"]))
			asks := make([][]float64, 0, len(orderBookState["ask"]))

			// Собираем цены в слайсы для сортировки
			for price, volume := range orderBookState["bid"] {
				bids = append(bids, []float64{price, volume})
			}
			for price, volume := range orderBookState["ask"] {
				asks = append(asks, []float64{price, volume})
			}

			// Сортируем цены
			sort.Slice(bids, func(i, j int) bool {
				return bids[i][0] > bids[j][0] // По убыванию для бидов
			})
			sort.Slice(asks, func(i, j int) bool {
				return asks[i][0] < asks[j][0] // По возрастанию для асков
			})

			// Аккумулируем объемы
			accumulatedBids := make([][]float64, len(bids))
			var totalBidVolume float64
			for i := 0; i < len(bids); i++ { // Аккумулируем от высоких цен к низким для бидов
				totalBidVolume += bids[i][1]
				accumulatedBids[i] = []float64{bids[i][0], totalBidVolume}
			}

			accumulatedAsks := make([][]float64, len(asks))
			var totalAskVolume float64
			for i := 0; i < len(asks); i++ { // Аккумулируем от низких цен к высоким для асков
				totalAskVolume += asks[i][1]
				accumulatedAsks[i] = []float64{asks[i][0], totalAskVolume}
			}

			// Ограничиваем количество уровней один раз в конце
			maxLevels := 100
			if len(accumulatedBids) > maxLevels {
				accumulatedBids = accumulatedBids[:maxLevels]
			}
			if len(accumulatedAsks) > maxLevels {
				accumulatedAsks = accumulatedAsks[:maxLevels]
			}

			// Отправляем сообщение
			msg := models.WSMessage{
				Type:        models.WSTypeOrderBook,
				Exchange:    sub.Exchange,
				TradingPair: sub.TradingPair,
				Interval:    sub.Interval,
				Timestamp:   time.Now().Format(time.RFC3339),
				Data: struct {
					Bids [][]float64 `json:"bids"`
					Asks [][]float64 `json:"asks"`
				}{
					Bids: accumulatedBids,
					Asks: accumulatedAsks,
				},
			}

			if err := conn.SendMessage(msg); err != nil {
				log.Error().
					Err(err).
					Str("clientID", conn.GetClientID()).
					Msg("Failed to send order book update")
				return
			}

			log.Debug().
				Str("clientID", conn.GetClientID()).
				Str("exchange", sub.Exchange).
				Str("pair", sub.TradingPair).
				Int("bids", len(bids)).
				Int("asks", len(asks)).
				Msg("Order book full update sent")
		}
	}
}

func (sm *SubscriptionManager) streamOrderBookAgg(ctx context.Context, conn *Connection, sub models.Subscription) {
	log.Info().
		Str("clientID", conn.GetClientID()).
		Str("exchange", sub.Exchange).
		Str("tradingPair", sub.TradingPair).
		Str("interval", sub.Interval).
		Msg("Starting order book aggregation stream")

	// Создаем контекст с возможностью отмены
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Запускаем горутину для мониторинга состояния соединения
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-streamCtx.Done():
				return
			case <-ticker.C:
				if conn.IsClosed() {
					cancel()
					return
				}
			}
		}
	}()

	// Определяем интервал запросов на основе интервала подписки
	queryInterval := 5 * time.Second
	if sub.Interval == "1m" {
		queryInterval = 10 * time.Second
	} else if sub.Interval == "5m" {
		queryInterval = 30 * time.Second
	} else if sub.Interval == "15m" || sub.Interval == "30m" || sub.Interval == "1h" {
		queryInterval = 60 * time.Second
	}

	ticker := time.NewTicker(queryInterval)
	defer ticker.Stop()

	transformer := service.NewDataTransformer()
	var lastQueryTime time.Time
	var lastSentDataHash string // Для отслеживания отправленных данных

	// Отправляем начальные данные при подписке
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().
					Interface("recover", r).
					Str("clientID", conn.GetClientID()).
					Msg("Recovered from panic in initial order book agg data")
			}
		}()

		// Настраиваем параметры запроса для начальных данных
		params := influx.QueryParams{
			StartTime:   time.Now().Add(-10 * time.Minute),
			EndTime:     time.Now(),
			ClientID:    conn.GetClientID(),
			Exchange:    sub.Exchange,
			TradingPair: sub.TradingPair,
			Interval:    sub.Interval,
			Bucket:      sm.influxClient.GetBucket("orderbook_agg"),
		}

		initialData, err := sm.influxClient.GetOrderBookAggData(streamCtx, params)
		if err != nil {
			log.Error().
				Err(err).
				Str("clientID", conn.GetClientID()).
				Str("exchange", sub.Exchange).
				Str("pair", sub.TradingPair).
				Msg("Failed to get initial order book aggregation data")
		} else if len(initialData) > 0 {
			// Если получены данные, преобразуем их и отправляем клиенту
			aggData := transformer.TransformOrderBookAgg(initialData)
			if aggData != nil {
				if conn.IsClosed() {
					return
				}

				msg := models.WSMessage{
					Type:        models.WSTypeOrderBookAgg,
					Exchange:    sub.Exchange,
					TradingPair: sub.TradingPair,
					Interval:    sub.Interval,
					Timestamp:   time.Now().Format(time.RFC3339),
					Data:        aggData,
				}

				if err := conn.SendMessage(msg); err != nil {
					log.Error().
						Err(err).
						Str("clientID", conn.GetClientID()).
						Msg("Failed to send initial orderbook agg data")
					return
				}

				// Сохраняем хеш данных
				dataBytes, _ := json.Marshal(aggData)
				lastSentDataHash = fmt.Sprintf("%x", md5.Sum(dataBytes))

				// Обновляем время последнего запроса
				if len(initialData) > 0 {
					lastPoint := initialData[len(initialData)-1]
					lastQueryTime = lastPoint.Timestamp
				}

				log.Debug().
					Str("clientID", conn.GetClientID()).
					Str("exchange", sub.Exchange).
					Str("pair", sub.TradingPair).
					Str("dataHash", lastSentDataHash).
					Int("dataPoints", len(initialData)).
					Msg("Initial order book aggregation data sent")
			}
		}
	}()

	for {
		select {
		case <-streamCtx.Done():
			log.Info().
				Str("clientID", conn.GetClientID()).
				Str("exchange", sub.Exchange).
				Str("tradingPair", sub.TradingPair).
				Msg("Stopping order book aggregation stream")
			return

		case <-ticker.C:
			if conn.IsClosed() {
				return
			}

			currentTime := time.Now()

			// Определяем временной диапазон для запроса
			var startTime time.Time
			if lastQueryTime.IsZero() {
				// Если это первый запрос, берем данные за последние несколько минут
				startTime = currentTime.Add(-5 * time.Minute)
			} else {
				// Иначе запрашиваем данные начиная с момента последнего запроса
				startTime = lastQueryTime.Add(time.Nanosecond)
			}

			params := influx.QueryParams{
				StartTime:   startTime,
				EndTime:     currentTime,
				ClientID:    conn.GetClientID(),
				Exchange:    sub.Exchange,
				TradingPair: sub.TradingPair,
				Interval:    sub.Interval,
				Bucket:      sm.influxClient.GetBucket("orderbook_agg"),
			}

			log.Debug().
				Str("clientID", conn.GetClientID()).
				Str("exchange", sub.Exchange).
				Str("pair", sub.TradingPair).
				Str("interval", sub.Interval).
				Time("startTime", startTime).
				Time("endTime", currentTime).
				Msg("Querying order book aggregation data")

			data, err := sm.influxClient.GetOrderBookAggData(streamCtx, params)
			if err != nil {
				log.Error().
					Err(err).
					Str("clientID", conn.GetClientID()).
					Str("exchange", sub.Exchange).
					Str("pair", sub.TradingPair).
					Time("startTime", startTime).
					Time("endTime", currentTime).
					Msg("Failed to get order book aggregation data")
				continue
			}

			if len(data) > 0 {
				// Защита от паники при трансформации
				var aggData interface{}
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Error().
								Interface("recover", r).
								Str("clientID", conn.GetClientID()).
								Msg("Recovered from panic in transform order book agg")
						}
					}()

					aggData = transformer.TransformOrderBookAgg(data)
				}()

				if aggData != nil {
					// Проверяем, изменились ли данные
					dataBytes, _ := json.Marshal(aggData)
					dataHash := fmt.Sprintf("%x", md5.Sum(dataBytes))

					// Отправляем только если данные изменились
					if dataHash != lastSentDataHash {
						msg := models.WSMessage{
							Type:        models.WSTypeOrderBookAgg,
							Exchange:    sub.Exchange,
							TradingPair: sub.TradingPair,
							Interval:    sub.Interval,
							Timestamp:   currentTime.Format(time.RFC3339),
							Data:        aggData,
						}

						if err := conn.SendMessage(msg); err != nil {
							log.Error().
								Err(err).
								Str("clientID", conn.GetClientID()).
								Msg("Failed to send order book aggregation update")
							return
						}

						// Сохраняем хеш отправленных данных
						lastSentDataHash = dataHash

						// Обновляем время последнего запроса
						lastPoint := data[len(data)-1]
						lastQueryTime = lastPoint.Timestamp

						log.Debug().
							Str("clientID", conn.GetClientID()).
							Str("exchange", sub.Exchange).
							Str("pair", sub.TradingPair).
							Str("dataHash", dataHash).
							Int("dataPoints", len(data)).
							Time("lastTimestamp", lastQueryTime).
							Msg("Order book aggregation update sent")
					} else {
						log.Debug().
							Str("clientID", conn.GetClientID()).
							Str("exchange", sub.Exchange).
							Str("pair", sub.TradingPair).
							Msg("Skipped duplicate order book aggregation data")
					}
				} else {
					log.Warn().
						Str("clientID", conn.GetClientID()).
						Msg("Transform returned nil data")
				}
			} else {
				log.Debug().
					Str("clientID", conn.GetClientID()).
					Str("exchange", sub.Exchange).
					Str("pair", sub.TradingPair).
					Msg("No new order book aggregation data")
			}
		}
	}
}

// getInitialOrderBookAggData получает начальные данные для агрегированного стакана
func (sm *SubscriptionManager) getInitialOrderBookAggData(ctx context.Context, conn *Connection, sub models.Subscription) ([]models.MarketData, error) {
	// Настраиваем параметры запроса для начальных данных
	params := influx.QueryParams{
		StartTime:   time.Now().Add(-10 * time.Minute),
		EndTime:     time.Now(),
		ClientID:    conn.GetClientID(),
		Exchange:    sub.Exchange,
		TradingPair: sub.TradingPair,
		Interval:    sub.Interval,
		Bucket:      sm.influxClient.GetBucket("orderbook_agg"),
	}

	// Получаем агрегированные данные стакана
	return sm.influxClient.GetOrderBookAggData(ctx, params)
}

func (sm *SubscriptionManager) handleCandlesSubscription(ctx context.Context, conn *Connection, sub models.Subscription) error {
	transformer := service.NewDataTransformer()

	// Получаем начальные данные
	params := influx.QueryParams{
		StartTime:   time.Now().Add(-1 * time.Hour),
		EndTime:     time.Now(),
		ClientID:    conn.GetClientID(),
		Exchange:    sub.Exchange,
		TradingPair: sub.TradingPair,
		Bucket:      sm.influxClient.GetBucket("candles"),
	}

	log.Debug().
		Str("clientID", conn.GetClientID()).
		Str("exchange", sub.Exchange).
		Str("pair", sub.TradingPair).
		Msg("Starting candles subscription")

	data, err := sm.influxClient.GetHistoricalData(ctx, params)
	if err != nil {
		return err
	}

	// Отправляем начальные данные
	candleData := transformer.TransformCandles(data)
	initialMsg := models.WSMessage{
		Type:        models.WSTypeCandles,
		Exchange:    sub.Exchange,
		TradingPair: sub.TradingPair,
		Interval:    sub.Interval,
		Timestamp:   time.Now().Format(time.RFC3339),
		Data:        candleData,
	}

	if err := conn.SendMessage(initialMsg); err != nil {
		return err
	}

	// Запускаем горутину для обновлений
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		log.Info().
			Str("clientID", conn.GetClientID()).
			Str("exchange", sub.Exchange).
			Str("pair", sub.TradingPair).
			Msg("Started real-time candles updates")

		for {
			select {
			case <-ctx.Done():
				log.Info().
					Str("clientID", conn.GetClientID()).
					Str("exchange", sub.Exchange).
					Str("pair", sub.TradingPair).
					Msg("Stopping candles subscription")
				return
			case t := <-ticker.C:
				params.StartTime = t.Add(-1 * time.Second)
				params.EndTime = t

				log.Debug().
					Str("clientID", conn.GetClientID()).
					Str("exchange", sub.Exchange).
					Str("pair", sub.TradingPair).
					Time("startTime", params.StartTime).
					Time("endTime", params.EndTime).
					Msg("Requesting new candle data")

				newData, err := sm.influxClient.GetHistoricalData(ctx, params)
				if err != nil {
					log.Error().
						Err(err).
						Str("clientID", conn.GetClientID()).
						Str("exchange", sub.Exchange).
						Str("pair", sub.TradingPair).
						Msg("Failed to get real-time candle data")
					continue
				}

				if len(newData) > 0 {
					log.Debug().
						Str("clientID", conn.GetClientID()).
						Str("exchange", sub.Exchange).
						Str("pair", sub.TradingPair).
						Int("dataPoints", len(newData)).
						Msg("Received new candle data")

					updateMsg := models.WSMessage{
						Type:        models.WSTypeCandles,
						Exchange:    sub.Exchange,
						TradingPair: sub.TradingPair,
						Interval:    sub.Interval,
						Timestamp:   time.Now().Format(time.RFC3339),
						Data:        transformer.TransformCandles(newData),
					}

					if err := conn.SendMessage(updateMsg); err != nil {
						log.Error().
							Err(err).
							Str("clientID", conn.GetClientID()).
							Str("exchange", sub.Exchange).
							Str("pair", sub.TradingPair).
							Msg("Failed to send candle update")
						return
					}

					log.Debug().
						Str("clientID", conn.GetClientID()).
						Str("exchange", sub.Exchange).
						Str("pair", sub.TradingPair).
						Msg("Sent candle update")
				}
			}
		}
	}()

	return nil
}

func (sm *SubscriptionManager) handleOrderBookSubscription(ctx context.Context, conn *Connection, sub models.Subscription) error {
	transformer := service.NewDataTransformer()

	params := influx.QueryParams{
		StartTime:   time.Now().Add(-1 * time.Second),
		EndTime:     time.Now(),
		ClientID:    conn.GetClientID(),
		Exchange:    sub.Exchange,
		TradingPair: sub.TradingPair,
		Bucket:      sm.influxClient.GetBucket("orderbook"),
	}

	data, err := sm.influxClient.GetHistoricalData(ctx, params)
	if err != nil {
		return err
	}

	orderBookData := transformer.TransformOrderBook(data)
	msg := models.WSMessage{
		Type:        models.WSTypeOrderBook,
		Exchange:    sub.Exchange,
		TradingPair: sub.TradingPair,
		Interval:    sub.Interval,
		Timestamp:   time.Now().Format(time.RFC3339),
		Data:        orderBookData,
	}

	if err := conn.SendMessage(msg); err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				params.StartTime = time.Now().Add(-100 * time.Millisecond)
				params.EndTime = time.Now()

				newData, err := sm.influxClient.GetHistoricalData(ctx, params)
				if err != nil {
					log.Error().Err(err).Msg("Failed to get real-time orderbook data")
					continue
				}

				if len(newData) > 0 {
					updateMsg := models.WSMessage{
						Type:        models.WSTypeOrderBook,
						Exchange:    sub.Exchange,
						TradingPair: sub.TradingPair,
						Interval:    sub.Interval,
						Timestamp:   time.Now().Format(time.RFC3339),
						Data:        transformer.TransformOrderBook(newData),
					}

					if err := conn.SendMessage(updateMsg); err != nil {
						log.Error().Err(err).Msg("Failed to send orderbook update")
						return
					}
				}
			}
		}
	}()

	return nil
}

func (sm *SubscriptionManager) handleOrderBookAggSubscription(ctx context.Context, conn *Connection, sub models.Subscription) error {
	transformer := service.NewDataTransformer()

	params := influx.QueryParams{
		StartTime:   time.Now().Add(-5 * time.Minute),
		EndTime:     time.Now(),
		ClientID:    conn.GetClientID(),
		Exchange:    sub.Exchange,
		TradingPair: sub.TradingPair,
		Bucket:      sm.influxClient.GetBucket("orderbook_agg"),
	}

	data, err := sm.influxClient.GetHistoricalData(ctx, params)
	if err != nil {
		return err
	}

	aggData := transformer.TransformOrderBookAgg(data)
	msg := models.WSMessage{
		Type:        models.WSTypeOrderBookAgg,
		Exchange:    sub.Exchange,
		TradingPair: sub.TradingPair,
		Interval:    sub.Interval,
		Timestamp:   time.Now().Format(time.RFC3339),
		Data:        aggData,
	}

	if err := conn.SendMessage(msg); err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				params.StartTime = time.Now().Add(-1 * time.Second)
				params.EndTime = time.Now()

				newData, err := sm.influxClient.GetHistoricalData(ctx, params)
				if err != nil {
					log.Error().Err(err).Msg("Failed to get real-time orderbook aggregation data")
					continue
				}

				if len(newData) > 0 {
					updateMsg := models.WSMessage{
						Type:        models.WSTypeOrderBookAgg,
						Exchange:    sub.Exchange,
						TradingPair: sub.TradingPair,
						Interval:    sub.Interval,
						Timestamp:   time.Now().Format(time.RFC3339),
						Data:        transformer.TransformOrderBookAgg(newData),
					}

					if err := conn.SendMessage(updateMsg); err != nil {
						log.Error().Err(err).Msg("Failed to send orderbook aggregation update")
						return
					}
				}
			}
		}
	}()

	return nil
}

func (sm *SubscriptionManager) ProcessSubscriptionRequest(conn *Connection, req models.SubscriptionRequest) error {
	log.Info().
		Str("clientID", conn.GetClientID()).
		Str("action", req.Action).
		Interface("subscriptions", req.Subscriptions).
		Msg("Processing subscription request")

	switch req.Action {
	case "subscribe":
		if conn.IsClosed() {
			return fmt.Errorf("connection is closed")
		}

		for _, sub := range req.Subscriptions {
			// Создаем контекст с отменой для каждой подписки
			subCtx, cancel := context.WithCancel(context.Background())
			conn.AddCancelFunc(sub, cancel)

			switch sub.Type {
			case models.SubTypeCandles:
				go sm.streamCandles(subCtx, conn, sub)
			case models.SubTypeOrderBook:
				go sm.streamOrderBook(subCtx, conn, sub)
			case models.SubTypeOrderBookAgg:
				go sm.streamOrderBookAgg(subCtx, conn, sub)
			default:
				log.Warn().
					Str("clientID", conn.GetClientID()).
					Str("type", string(sub.Type)).
					Msg("Unknown subscription type")
				continue
			}

			// Добавляем подписку в менеджер
			sm.addSubscription(conn, sub)

			log.Info().
				Str("clientID", conn.GetClientID()).
				Str("type", string(sub.Type)).
				Str("exchange", sub.Exchange).
				Str("tradingPair", sub.TradingPair).
				Msg("Subscription started")
		}

	case "unsubscribe":
		for _, sub := range req.Subscriptions {
			// Находим и отменяем конкретные подписки
			sm.removeSubscription(conn, sub)

			// Отправляем подтверждение отписки
			msg := models.WSMessage{
				Type:        models.WSTypeUnsubscribe,
				Exchange:    sub.Exchange,
				TradingPair: sub.TradingPair,
				Timestamp:   time.Now().Format(time.RFC3339),
				Data: models.SubscriptionResponse{
					Success: true,
					Type:    sub.Type,
				},
			}

			if err := conn.SendMessage(msg); err != nil {
				log.Error().
					Err(err).
					Str("clientID", conn.GetClientID()).
					Str("type", string(sub.Type)).
					Msg("Failed to send unsubscribe confirmation")
			}

			log.Info().
				Str("clientID", conn.GetClientID()).
				Str("type", string(sub.Type)).
				Str("exchange", sub.Exchange).
				Str("tradingPair", sub.TradingPair).
				Msg("Subscription cancelled")
		}

	default:
		return fmt.Errorf("unknown action: %s", req.Action)
	}

	return nil
}

func (sm *SubscriptionManager) RemoveConnection(conn *Connection) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Закрываем соединение если еще не закрыто
	if !conn.IsClosed() {
		conn.Close()
	}

	// Удаляем из списка соединений
	delete(sm.connections, conn)

	log.Info().
		Str("clientID", conn.GetClientID()).
		Msg("Connection removed from subscription manager")
}
