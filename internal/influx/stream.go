package influx

import (
	"context"
	"fmt"
	"market-analytics-service/pkg/models"
	"time"

	"github.com/rs/zerolog/log"
)

type Stream interface {
    Subscribe(ctx context.Context, clientID string) (<-chan models.MarketData, error)
    Close() error
}

type stream struct {
    client   *Client
    interval time.Duration
    buffer   int
}

func NewStream(client *Client, interval time.Duration, buffer int) Stream {
    return &stream{
        client:   client,
        interval: interval,
        buffer:   buffer,
    }
}

func (s *stream) Subscribe(ctx context.Context, clientID string) (<-chan models.MarketData, error) {
    if clientID == "" {
        return nil, fmt.Errorf("clientID is required")
    }

    dataCh := make(chan models.MarketData, s.buffer)

    go func() {
        defer close(dataCh)

        ticker := time.NewTicker(s.interval)
        defer ticker.Stop()

        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                // Query for specific client's data
                fluxQuery := fmt.Sprintf(`
                    from(bucket:"%s")
                        |> range(start: -1s)
                        |> filter(fn: (r) => r["client_id"] == "%s")
                `,
                    s.client.bucket,
                    clientID,
                )

                log.Debug().
                    Str("query", fluxQuery).
                    Str("clientID", clientID).
                    Msg("Executing InfluxDB query")

                result, err := s.client.queryAPI.Query(ctx, fluxQuery)
                if err != nil {
                    log.Error().
                        Err(err).
                        Str("clientID", clientID).
                        Msg("Failed to query InfluxDB")
                    continue
                }

                for result.Next() {
                    record := result.Record()

                    data := models.MarketData{
                        Type:        safeGetString(record.Values(), "_measurement"),
                        Exchange:    safeGetString(record.Values(), "exchange"),
                        TradingPair: safeGetString(record.Values(), "trading_pair"),
                        Timestamp:   record.Time(),
                        ClientID:    clientID,
                        Data:        record.Values(),
                    }

                    log.Debug().
                        Str("clientID", clientID).
                        Str("type", data.Type).
                        Str("exchange", data.Exchange).
                        Str("tradingPair", data.TradingPair).
                        Time("timestamp", data.Timestamp).
                        Msg("Received data from InfluxDB")

                    select {
                    case dataCh <- data:
                    case <-ctx.Done():
                        result.Close()
                        return
                    default:
                        log.Warn().
                            Str("clientID", clientID).
                            Msg("Channel buffer full, skipping data point")
                    }
                }

                if result.Err() != nil {
                    log.Error().
                        Err(result.Err()).
                        Str("clientID", clientID).
                        Msg("Error processing InfluxDB results")
                }
                result.Close()
            }
        }
    }()

    return dataCh, nil
}

func safeGetString(values map[string]interface{}, key string) string {
    if val, ok := values[key]; ok {
        if strVal, ok := val.(string); ok {
            return strVal
        }
    }
    return ""
}

func (s *stream) Close() error {
    return nil
}
