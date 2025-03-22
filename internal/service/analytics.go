package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"market-analytics-service/internal/influx"
	"market-analytics-service/internal/postgres"
	"market-analytics-service/pkg/models"

	"github.com/rs/zerolog/log"
)

// AnalyticsService handles market data analytics and distribution
type AnalyticsService struct {
	stream         influx.Stream
	influxClient   *influx.Client
	postgresClient *postgres.Client
	subscribers    sync.Map
	cancel         context.CancelFunc
}

// NewAnalyticsService creates a new analytics service instance
func NewAnalyticsService(influxClient *influx.Client, postgresClient *postgres.Client) *AnalyticsService {
	stream := influx.NewStream(
		influxClient,
		100*time.Millisecond,
		1000,
	)

	svc := &AnalyticsService{
		stream:         stream,
		influxClient:   influxClient, // Store influxClient
		postgresClient: postgresClient,
		subscribers:    sync.Map{},
	}

	_, cancel := context.WithCancel(context.Background())
	svc.cancel = cancel

	return svc
}

// Subscribe creates a new subscription for market data
func (s *AnalyticsService) Subscribe(ctx context.Context, clientID string) (<-chan models.MarketData, error) {
	if clientID == "" {
		return nil, fmt.Errorf("clientID is required")
	}

	log.Debug().
		Str("clientID", clientID).
		Msg("Creating subscription")

	// Create data channel with the stream
	dataCh, err := s.stream.Subscribe(ctx, clientID)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	// Store subscription for management
	s.subscribers.Store(clientID, dataCh)

	return dataCh, nil
}

// resolveClientIDs determines which client IDs to subscribe to
func (s *AnalyticsService) resolveClientIDs(ctx context.Context, userID, clientID string) ([]string, error) {
	if clientID != "" {
		return []string{clientID}, nil
	}

	if userID != "" {
		// Fetch all client IDs associated with the user
		clientIDs, err := s.postgresClient.GetUserClientIDs(ctx, userID)
		if err != nil {
			log.Error().Err(err).Str("userID", userID).Msg("Failed to get user client IDs")
			return []string{}, nil
		}
		return clientIDs, nil
	}

	// If neither userID nor clientID provided, return empty slice for public data
	return []string{}, nil
}

// buildClientIDsQuery creates a query filter for multiple client IDs
func (s *AnalyticsService) buildClientIDsQuery(clientIDs []string) string {
	if len(clientIDs) == 0 {
		return `r["client_id"] == ""` // Query for public data only
	}

	// Build query for specific client IDs
	conditions := make([]string, len(clientIDs)+1) // +1 for public data
	conditions[0] = `r["client_id"] == ""`         // Always include public data

	for i, id := range clientIDs {
		conditions[i+1] = fmt.Sprintf(`r["client_id"] == "%s"`, id)
	}

	return strings.Join(conditions, " or ")
}

// createSubscriptionKey generates a unique key for subscription management
func (s *AnalyticsService) createSubscriptionKey(userID string, clientIDs []string) string {
	if len(clientIDs) == 0 {
		return "public"
	}
	if len(clientIDs) == 1 {
		return clientIDs[0]
	}
	return fmt.Sprintf("%s:all", userID)
}

// Unsubscribe removes a subscription
func (s *AnalyticsService) Unsubscribe(subKey string, ch <-chan models.MarketData) {
	if value, ok := s.subscribers.LoadAndDelete(subKey); ok {
		if dataCh, ok := value.(<-chan models.MarketData); ok {
			if dataCh == ch {
				// Channel will be closed by the stream when context is cancelled
				log.Debug().
					Str("subKey", subKey).
					Msg("Subscription removed")
			}
		}
	}
}

// GetHistoricalData retrieves historical market data
func (s *AnalyticsService) GetHistoricalData(ctx context.Context, clientID string, startTime, endTime time.Time) ([]models.MarketData, error) {
	params := influx.QueryParams{
		ClientID:  clientID,
		StartTime: startTime,
		EndTime:   endTime,
	}

	data, err := s.influxClient.GetHistoricalData(ctx, params) // Now we can use s.influxClient
	if err != nil {
		return nil, fmt.Errorf("failed to get historical data: %w", err)
	}

	return data, nil
}

// Close performs cleanup of service resources
func (s *AnalyticsService) Close() error {
	// Cancel all ongoing operations
	if s.cancel != nil {
		s.cancel()
	}

	// Close the stream
	if err := s.stream.Close(); err != nil {
		return fmt.Errorf("failed to close stream: %w", err)
	}

	// Clear all subscriptions
	s.subscribers.Range(func(key, value interface{}) bool {
		s.subscribers.Delete(key)
		return true
	})

	return nil
}
