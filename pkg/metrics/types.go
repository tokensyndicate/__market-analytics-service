package metrics

import (
	"context"
	"time"

	"market-analytics-service/pkg/models"
)

// MetricsProvider defines interface for metrics calculation
type MetricsProvider interface {
    // Calculate metrics for specific market
    CalculateMetrics(ctx context.Context, params CalculationParams) (*models.MarketMetrics, error)

    // Subscribe to real-time metrics updates
    SubscribeMetrics(ctx context.Context, params SubscriptionParams) (<-chan *models.MarketMetrics, error)
}

// CalculationParams defines parameters for metrics calculation
type CalculationParams struct {
    Exchange    string
    TradingPair string
    ClientID    string
    StartTime   time.Time
    EndTime     time.Time
}

// SubscriptionParams defines parameters for metrics subscription
type SubscriptionParams struct {
    Exchange     string
    TradingPair  string
    ClientID     string
    UpdatePeriod time.Duration
}
