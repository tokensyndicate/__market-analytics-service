package influx

import (
	"context"
	"fmt"
	"time"

	"market-analytics-service/pkg/models"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

// Client wraps InfluxDB client functionality
type Client struct {
	client   influxdb2.Client
	queryAPI api.QueryAPI // Исправленный тип
	org      string
	bucket   string
}

// NewClient creates and initializes a new InfluxDB client
func NewClient(url, token, org, bucket string) (*Client, error) {
	client := influxdb2.NewClient(url, token)

	// Verify connection
	_, err := client.Ping(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to InfluxDB: %w", err)
	}

	return &Client{
		client:   client,
		queryAPI: client.QueryAPI(org), // QueryAPI возвращает api.QueryAPI
		org:      org,
		bucket:   bucket,
	}, nil
}

// Close releases the client resources
func (c *Client) Close() {
	c.client.Close()
}

// GetHistoricalData retrieves historical data for a specific time period
func (c *Client) GetHistoricalData(ctx context.Context, params QueryParams) ([]models.MarketData, error) {
	// Build Flux query
	query := fmt.Sprintf(`
        from(bucket:"%s")
            |> range(start: %s, stop: %s)
            |> filter(fn: (r) => r["client_id"] == "%s" or r["client_id"] == "")
    `,
		c.bucket,
		params.StartTime.Format(time.RFC3339),
		params.EndTime.Format(time.RFC3339),
		params.ClientID,
	)

	// Execute query
	result, err := c.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer result.Close()

	var data []models.MarketData
	for result.Next() {
		record := result.Record()

		marketData := models.MarketData{
			Type:        record.Measurement(),
			Exchange:    record.ValueByKey("exchange").(string),
			TradingPair: record.ValueByKey("trading_pair").(string),
			Timestamp:   record.Time(),
			ClientID:    record.ValueByKey("client_id").(string),
			Data:        record.Values(),
		}
		data = append(data, marketData)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("error during query execution: %w", result.Err())
	}

	return data, nil
}

// SubscribeToData subscribes to real-time market data updates
func (c *Client) SubscribeToData(ctx context.Context, clientID string) (<-chan models.MarketData, error) {
	dataCh := make(chan models.MarketData, 100)

	query := fmt.Sprintf(`
        from(bucket:"%s")
            |> range(start: -1s)
            |> filter(fn: (r) => r["client_id"] == "%s" or r["client_id"] == "")
    `, c.bucket, clientID)

	go func() {
		defer close(dataCh)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				result, err := c.queryAPI.Query(ctx, query)
				if err != nil {
					time.Sleep(time.Second)
					continue
				}

				for result.Next() {
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

				time.Sleep(time.Millisecond * 100)
			}
		}
	}()

	return dataCh, nil
}

// QueryParams represents parameters for historical data queries
type QueryParams struct {
	ClientID  string
	StartTime time.Time
	EndTime   time.Time
}
