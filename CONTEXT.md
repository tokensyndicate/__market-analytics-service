# TS Analytics Service

## Overview

Analytics service provides market data for dashboards via WebSocket for real-time updates and HTTP for historical data requests.

## Key Components

### Data Flow

1. Market Monitor service collects and stores data in InfluxDB
2. Analytics service subscribes to InfluxDB for real-time updates
3. Analytics service filters data based on client_id from request headers
4. Data is streamed to clients via WebSocket or returned via HTTP API

### Authentication

- client_id is required in request headers
- If client_id is empty, only common data is returned
- If client_id is provided, both common data and client-specific data are returned

### APIs

#### WebSocket `/ws`

- Real-time market data streaming
- Requires client_id in headers
- One-way communication (server to client only)
- Automatically filters data based on client_id

#### HTTP `/api/v1/historical`

- Historical data retrieval
- Query parameters:
  - start_time (RFC3339)
  - end_time (RFC3339)
- Requires client_id in headers
- Returns filtered data for the specified time period

### Data Models

#### Market Data

```go
type MarketData struct {
    Type        string      `json:"type"`
    Exchange    string      `json:"exchange"`
    TradingPair string      `json:"trading_pair"`
    Timestamp   time.Time   `json:"timestamp"`
    ClientID    string      `json:"client_id"`
    Data        interface{} `json:"data"`
}
```

### Configuration

```yaml
server:
  host: "0.0.0.0"
  http_port: 8080
  ws_port: 8081

influxdb:
  url: "http://localhost:8086"
  token: "your-token-here"
  org: "your-org"
  bucket: "trading"
```

### Environment Variables

All configuration can be overridden using environment variables with prefix `TS_ANALYTICS_`:

- TS_ANALYTICS_HOST
- TS_ANALYTICS_HTTP_PORT
- TS_ANALYTICS_WS_PORT
- TS_ANALYTICS_INFLUXDB_URL
- TS_ANALYTICS_INFLUXDB_TOKEN
- TS_ANALYTICS_INFLUXDB_ORG
- TS_ANALYTICS_INFLUXDB_BUCKET

## Project Structure

```
market-analytics-service/
├── cmd/
│   └── server/
│       └── main.go           # Application entry point
├── config/
│   └── config.yaml          # Configuration file
├── internal/
│   ├── config/
│   │   └── config.go        # Configuration handling
│   ├── influx/
│   │   └── client.go        # InfluxDB client
│   ├── server/
│   │   ├── server.go        # Server setup
│   │   ├── websocket.go     # WebSocket handlers
│   │   └── http.go          # HTTP handlers
│   └── service/
│       └── analytics.go      # Business logic
└── pkg/
    └── models/
        └── market.go        # Data models
```

## Dependencies

- influxdb-client-go/v2: InfluxDB client
- gorilla/websocket: WebSocket implementation
- gorilla/mux: HTTP router
- spf13/viper: Configuration management
- zerolog: Logging

## Running the Service

1. Setup configuration in config.yaml or environment variables
2. Run the service:

```bash
make run
```

Or with Docker:

```bash
make docker-run
```
