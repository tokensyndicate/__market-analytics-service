package metrics

import (
	"context"
	"fmt"
	"time"

	"market-analytics-service/internal/influx"
	"market-analytics-service/pkg/models"
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
		queries:      NewFluxQueries(influxClient.GetBucket("candles")),
	}
}

// CalculateMetrics implements MetricsProvider interface
func (p *DefaultMetricsProvider) CalculateMetrics(ctx context.Context, params CalculationParams) (*models.MarketMetrics, error) {
	metrics := &models.MarketMetrics{
		Exchange:    params.Exchange,
		TradingPair: params.TradingPair,
		ClientID:    params.ClientID,
		Timestamp:   time.Now(),
	}

	if err := p.calculatePriceMetrics(ctx, metrics, params); err != nil {
		return nil, fmt.Errorf("failed to calculate price metrics: %w", err)
	}

	if err := p.calculateVolumeMetrics(ctx, metrics, params); err != nil {
		return nil, fmt.Errorf("failed to calculate volume metrics: %w", err)
	}

	if err := p.calculateOrderBookMetrics(ctx, metrics, params); err != nil {
		return nil, fmt.Errorf("failed to calculate order book metrics: %w", err)
	}

	return metrics, nil
}

func (p *DefaultMetricsProvider) calculatePriceMetrics(ctx context.Context, metrics *models.MarketMetrics, params CalculationParams) error {
	query := p.queries.GetPriceMetricsQuery(params)
	result, err := p.influxClient.GetQueryAPI().Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query price metrics: %w", err)
	}
	defer result.Close()

	for result.Next() {
		record := result.Record()
		switch record.Field() {
		case "high":
			metrics.HighPrice24h = record.Value().(string)
		case "low":
			metrics.LowPrice24h = record.Value().(string)
		case "last":
			metrics.LastPrice = record.Value().(string)
		case "price_change":
			metrics.PriceChange = record.Value().(string)
		}
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

	for result.Next() {
		record := result.Record()
		switch record.Field() {
		case "volume":
			metrics.Volume24h = record.Value().(string)
		case "volume_change":
			metrics.VolumeChange = record.Value().(string)
		case "trade_count":
			metrics.TradeCount24h = int(record.Value().(int64))
		}
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

	for result.Next() {
		record := result.Record()
		switch record.Field() {
		case "spread":
			metrics.BidAskSpread = record.Value().(string)
		case "depth":
			metrics.MarketDepth = record.Value().(string)
		case "liquidity":
			metrics.Liquidity = record.Value().(string)
		}
	}

	return result.Err()
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
