package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"market-analytics-service/internal/influx"
	"market-analytics-service/pkg/models"
)

// AnalyticsService handles business logic for market data analytics
type AnalyticsService struct {
	influxClient *influx.Client
	subscribers  map[string][]chan models.MarketData
	mu           sync.RWMutex
	cancel       context.CancelFunc
}

// NewAnalyticsService creates a new analytics service instance
func NewAnalyticsService(influxClient *influx.Client) *AnalyticsService {
	svc := &AnalyticsService{
		influxClient: influxClient,
		subscribers:  make(map[string][]chan models.MarketData),
	}

	// Используем контекст с отменой
	ctx, cancel := context.WithCancel(context.Background())
	go svc.distributeData(ctx)

	// Сохраняем cancel для корректного завершения
	svc.cancel = cancel

	return svc
}

// Subscribe creates a new subscription for market data
func (s *AnalyticsService) Subscribe(clientID string) (<-chan models.MarketData, error) {
	dataChan := make(chan models.MarketData, 100)

	s.mu.Lock()
	if _, exists := s.subscribers[clientID]; !exists {
		s.subscribers[clientID] = make([]chan models.MarketData, 0)
	}
	s.subscribers[clientID] = append(s.subscribers[clientID], dataChan)
	s.mu.Unlock()

	return dataChan, nil
}

// Unsubscribe removes a subscription
func (s *AnalyticsService) Unsubscribe(clientID string, ch <-chan models.MarketData) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if channels, exists := s.subscribers[clientID]; exists {
		for i, subCh := range channels {
			if subCh == ch {
				// Remove channel from slice
				close(subCh)
				s.subscribers[clientID] = append(channels[:i], channels[i+1:]...)
				break
			}
		}
		// If no more subscribers for this client, remove the client entry
		if len(s.subscribers[clientID]) == 0 {
			delete(s.subscribers, clientID)
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

	data, err := s.influxClient.GetHistoricalData(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to get historical data: %w", err)
	}

	return data, nil
}

// distributeData handles the distribution of real-time data to subscribers
func (s *AnalyticsService) distributeData(ctx context.Context) {
	// Keep track of active subscriptions per client
	subscriptions := make(map[string]<-chan models.MarketData)

	for {
		// Check and create subscriptions for clients
		s.mu.RLock()
		for clientID := range s.subscribers {
			if _, exists := subscriptions[clientID]; !exists {
				dataCh, err := s.influxClient.SubscribeToData(ctx, clientID)
				if err != nil {
					// Log error and continue
					continue
				}
				subscriptions[clientID] = dataCh
			}
		}
		s.mu.RUnlock()

		// Distribute data to subscribers
		for clientID, dataCh := range subscriptions {
			select {
			case data, ok := <-dataCh:
				if !ok {
					delete(subscriptions, clientID)
					continue
				}

				s.mu.RLock()
				subscribers := s.subscribers[clientID]
				s.mu.RUnlock()

				// Distribute to all subscribers for this client
				for _, sub := range subscribers {
					select {
					case sub <- data:
					default:
						// Skip if subscriber's channel is full
					}
				}
			case <-ctx.Done():
				return
			default:
				// Continue to next client if no data available
			}
		}
	}
}

// Close cleans up resources
func (s *AnalyticsService) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close all subscriber channels
	for _, channels := range s.subscribers {
		for _, ch := range channels {
			close(ch)
		}
	}
	s.subscribers = make(map[string][]chan models.MarketData)
}
