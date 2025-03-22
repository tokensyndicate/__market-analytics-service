package metrics

import (
	"fmt"
	"time"
)

// FluxQueries contains optimized Flux query templates
type FluxQueries struct {
	bucket string
}

// NewFluxQueries creates new FluxQueries instance
func NewFluxQueries(bucket string) *FluxQueries {
	return &FluxQueries{bucket: bucket}
}

// GetPriceMetricsQuery returns query for price-related metrics
func (q *FluxQueries) GetPriceMetricsQuery(params CalculationParams) string {
	return fmt.Sprintf(`
from(bucket: "%s")
    |> range(start: %s, stop: %s)
    |> filter(fn: (r) => r["_measurement"] == "trades")
    |> filter(fn: (r) => r["exchange"] == "%s" and r["trading_pair"] == "%s")
    |> filter(fn: (r) => r["client_id"] == "%s" or r["client_id"] == "")
    |> filter(fn: (r) => r["_field"] == "price")
    |> group(columns: ["_time"])
    |> reduce(
        fn: (r, accumulator) => ({
            high: if r._value > accumulator.high then r._value else accumulator.high,
            low: if r._value < accumulator.low then r._value else accumulator.low,
            last: r._value
        }),
        identity: {high: -1.0, low: 999999999.0, last: 0.0}
    )
`,
		q.bucket,
		params.StartTime.Format(time.RFC3339),
		params.EndTime.Format(time.RFC3339),
		params.Exchange,
		params.TradingPair,
		params.ClientID,
	)
}

// GetVolumeMetricsQuery returns query for volume-related metrics
func (q *FluxQueries) GetVolumeMetricsQuery(params CalculationParams) string {
	return fmt.Sprintf(`
from(bucket: "%s")
    |> range(start: %s, stop: %s)
    |> filter(fn: (r) => r["_measurement"] == "trades")
    |> filter(fn: (r) => r["exchange"] == "%s" and r["trading_pair"] == "%s")
    |> filter(fn: (r) => r["client_id"] == "%s" or r["client_id"] == "")
    |> filter(fn: (r) => r["_field"] == "volume")
    |> group()
    |> sum()
`,
		q.bucket,
		params.StartTime.Format(time.RFC3339),
		params.EndTime.Format(time.RFC3339),
		params.Exchange,
		params.TradingPair,
		params.ClientID,
	)
}

// GetOrderBookMetricsQuery returns query for order book metrics
func (q *FluxQueries) GetOrderBookMetricsQuery(params CalculationParams) string {
	return fmt.Sprintf(`
from(bucket: "%s")
    |> range(start: -1m)
    |> filter(fn: (r) => r["_measurement"] == "orderbook")
    |> filter(fn: (r) => r["exchange"] == "%s" and r["trading_pair"] == "%s")
    |> filter(fn: (r) => r["client_id"] == "%s" or r["client_id"] == "")
    |> filter(fn: (r) => r["_field"] == "price" or r["_field"] == "volume")
    |> last()
`,
		q.bucket,
		params.Exchange,
		params.TradingPair,
		params.ClientID,
	)
}
