package metrics

import (
	"context"
	"fmt"
	"time"

	"market-analytics-service/internal/influx"
	"market-analytics-service/pkg/models"

	"github.com/rs/zerolog/log"
)

// DefaultMetricsProvider implements MetricsProvider interface
type DefaultMetricsProvider struct {
	influxClient *influx.Client
	queries      *FluxQueries
}

// NewMetricsProvider creates new instance of metrics provider
func NewMetricsProvider(influxClient *influx.Client) MetricsProvider {
	return &DefaultMetricsProvider{
		influxClient: influxClient,
		queries:      NewFluxQueries(influxClient.GetBucket()),
	}
}

// CalculateMetrics implements MetricsProvider interface
func (p *DefaultMetricsProvider) CalculateMetrics(ctx context.Context, params CalculationParams) (*models.MarketMetrics, error) {
	// Create base metrics object
	metrics := &models.MarketMetrics{
		Exchange:    params.Exchange,
		TradingPair: params.TradingPair,
		ClientID:    params.ClientID,
		Timestamp:   time.Now(),
	}

	// Get price metrics
	if err := p.calculatePriceMetrics(ctx, metrics, params); err != nil {
		return nil, fmt.Errorf("failed to calculate price metrics: %w", err)
	}

	// Get volume metrics
	if err := p.calculateVolumeMetrics(ctx, metrics, params); err != nil {
		return nil, fmt.Errorf("failed to calculate volume metrics: %w", err)
	}

	// Get order book metrics
	if err := p.calculateOrderBookMetrics(ctx, metrics, params); err != nil {
		return nil, fmt.Errorf("failed to calculate order book metrics: %w", err)
	}

	return metrics, nil
}

// SubscribeMetrics implements MetricsProvider interface
func (p *DefaultMetricsProvider) SubscribeMetrics(ctx context.Context, params SubscriptionParams) (<-chan *models.MarketMetrics, error) {
	metricsChan := make(chan *models.MarketMetrics, 100)

	go func() {
		defer close(metricsChan)

		ticker := time.NewTicker(params.UpdatePeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				calcParams := CalculationParams{
					Exchange:    params.Exchange,
					TradingPair: params.TradingPair,
					ClientID:    params.ClientID,
					StartTime:   time.Now().Add(-24 * time.Hour),
					EndTime:     time.Now(),
				}

				metrics, err := p.CalculateMetrics(ctx, calcParams)
				if err != nil {
					log.Error().Err(err).
						Str("exchange", params.Exchange).
						Str("tradingPair", params.TradingPair).
						Msg("Failed to calculate metrics")
					continue
				}

				select {
				case metricsChan <- metrics:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return metricsChan, nil
}

// Internal helper methods for metrics calculation
func (p *DefaultMetricsProvider) calculatePriceMetrics(ctx context.Context, metrics *models.MarketMetrics, params CalculationParams) error {
	query := p.queries.GetPriceMetricsQuery(params)
	result, err := p.influxClient.GetQueryAPI().Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query price metrics: %w", err)
	}
	defer result.Close()

	var (
		high      float64
		low       float64
		lastPrice float64
		prevPrice float64
	)

	// Process results
	for result.Next() {
		record := result.Record()
		switch record.Field() {
		case "high":
			high = record.Value().(float64)
		case "low":
			low = record.Value().(float64)
		case "last":
			if record.Time().Before(time.Now().Add(-24 * time.Hour)) {
				prevPrice = record.Value().(float64)
			} else {
				lastPrice = record.Value().(float64)
			}
		}
	}

	metrics.LastPrice = lastPrice
	metrics.HighPrice24h = high
	metrics.LowPrice24h = low

	// Calculate price change percentage
	if prevPrice > 0 {
		metrics.PriceChange = ((lastPrice - prevPrice) / prevPrice) * 100
	}

	return result.Err()
}

func (p *DefaultMetricsProvider) calculateVolumeMetrics(ctx context.Context, metrics *models.MarketMetrics, params CalculationParams) error {
	query := p.queries.GetVolumeMetricsQuery(params)
	result, err := p.influxClient.GetQueryAPI().Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query volume metrics: %w", err)
	}
	defer result.Close()

	var (
		volume24h     float64
		prevVolume    float64
		tradeCount24h int
	)

	// Process results
	for result.Next() {
		record := result.Record()
		timestamp := record.Time()

		if timestamp.After(time.Now().Add(-24 * time.Hour)) {
			volume24h += record.ValueByKey("total").(float64)
			tradeCount24h += int(record.ValueByKey("count").(int64))
		} else {
			prevVolume += record.ValueByKey("total").(float64)
		}
	}

	metrics.Volume24h = volume24h
	metrics.TradeCount24h = tradeCount24h

	// Calculate volume change percentage
	if prevVolume > 0 {
		metrics.VolumeChange = ((volume24h - prevVolume) / prevVolume) * 100
	}

	return result.Err()
}

func (p *DefaultMetricsProvider) calculateOrderBookMetrics(ctx context.Context, metrics *models.MarketMetrics, params CalculationParams) error {
	query := p.queries.GetOrderBookMetricsQuery(params)
	result, err := p.influxClient.GetQueryAPI().Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query order book metrics: %w", err)
	}
	defer result.Close()

	var (
		bestBid     float64
		bestAsk     float64
		totalVolume float64
		midPrice    float64
	)

	// Process results
	for result.Next() {
		record := result.Record()
		side := record.ValueByKey("side").(string)

		switch record.Field() {
		case "price":
			price := record.Value().(float64)
			if side == "bid" && (bestBid == 0 || price > bestBid) {
				bestBid = price
			}
			if side == "ask" && (bestAsk == 0 || price < bestAsk) {
				bestAsk = price
			}
		case "volume":
			totalVolume += record.Value().(float64)
		}
	}

	// Calculate metrics
	if bestAsk > 0 && bestBid > 0 {
		metrics.BidAskSpread = bestAsk - bestBid
		midPrice = (bestAsk + bestBid) / 2

		// Calculate liquidity within ±2% of mid price
		metrics.Liquidity = p.calculateLiquidity(ctx, params, midPrice)
	}

	metrics.MarketDepth = totalVolume

	return result.Err()
}

// Helper method for liquidity calculation
func (p *DefaultMetricsProvider) calculateLiquidity(ctx context.Context, params CalculationParams, midPrice float64) float64 {
	// Calculate liquidity within ±2% of mid price
	lowerBound := midPrice * 0.98
	upperBound := midPrice * 1.02

	query := fmt.Sprintf(`
        from(bucket: "%s")
            |> range(start: -1m)
            |> filter(fn: (r) => r["_measurement"] == "orderbook")
            |> filter(fn: (r) => r["exchange"] == "%s" and r["trading_pair"] == "%s")
            |> filter(fn: (r) => r["client_id"] == "%s" or r["client_id"] == "")
            |> filter(fn: (r) => r["_field"] == "volume")
            |> filter(fn: (r) => r["price"] >= %f and r["price"] <= %f)
            |> sum()
    `, p.influxClient.GetBucket(), params.Exchange, params.TradingPair, params.ClientID, lowerBound, upperBound)

	result, err := p.influxClient.GetQueryAPI().Query(ctx, query)
	if err != nil {
		return 0
	}
	defer result.Close()

	var liquidity float64
	if result.Next() {
		liquidity = result.Record().Value().(float64)
	}

	return liquidity
}
