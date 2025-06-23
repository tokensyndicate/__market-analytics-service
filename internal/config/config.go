package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	Server         ServerConfig         `mapstructure:"server"`
	InfluxDB       InfluxDBConfig       `mapstructure:"influxdb"`
	MarketAnalysis MarketAnalysisConfig `mapstructure:"market_analysis"`
}

type ServerConfig struct {
	HTTPPort int    `mapstructure:"http_port"`
	WSPort   int    `mapstructure:"ws_port"`
	Host     string `mapstructure:"host"`
}

type InfluxDBConfig struct {
	URL     string  `mapstructure:"url"`
	Token   string  `mapstructure:"token"`
	Org     string  `mapstructure:"org"`
	Buckets Buckets `mapstructure:"buckets"`
}

type Buckets struct {
	Candles        string `mapstructure:"candles"`
	OrderBook      string `mapstructure:"orderbook"`
	OrderBookAgg   string `mapstructure:"orderbook_agg"`
	MarketAnalysis string `mapstructure:"market_analysis"`
}

// MarketAnalysisConfig defines settings for market maker analysis
type MarketAnalysisConfig struct {
	Enabled             bool    `mapstructure:"enabled"`
	TimeWindowMinutes   int     `mapstructure:"time_window_minutes"`
	MinOrderBookDepth   int     `mapstructure:"min_orderbook_depth"`
	MinCandleCount      int     `mapstructure:"min_candle_count"`
	ConfidenceThreshold float64 `mapstructure:"confidence_threshold"`
	AnalysisInterval    int     `mapstructure:"analysis_interval_minutes"`
}

func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")

	// Default values
	viper.SetDefault("server.host", "0.0.0.0")
	viper.SetDefault("server.http_port", 8080)
	viper.SetDefault("server.ws_port", 8081)
	
	// Market analysis defaults
	viper.SetDefault("market_analysis.enabled", true)
	viper.SetDefault("market_analysis.time_window_minutes", 60)
	viper.SetDefault("market_analysis.min_orderbook_depth", 10)
	viper.SetDefault("market_analysis.min_candle_count", 20)
	viper.SetDefault("market_analysis.confidence_threshold", 0.7)
	viper.SetDefault("market_analysis.analysis_interval_minutes", 15)

	// Support environment variables
	viper.AutomaticEnv()
	viper.SetEnvPrefix("TS_ANALYTICS")

	// Mapping environment variables
	viper.BindEnv("server.host", "TS_ANALYTICS_HOST")
	viper.BindEnv("server.http_port", "TS_ANALYTICS_HTTP_PORT")
	viper.BindEnv("server.ws_port", "TS_ANALYTICS_WS_PORT")
	viper.BindEnv("influxdb.url", "TS_ANALYTICS_INFLUXDB_URL")
	viper.BindEnv("influxdb.token", "TS_ANALYTICS_INFLUXDB_TOKEN")
	viper.BindEnv("influxdb.org", "TS_ANALYTICS_INFLUXDB_ORG")
	viper.BindEnv("influxdb.bucket", "TS_ANALYTICS_INFLUXDB_BUCKET")
	viper.BindEnv("market_analysis.enabled", "TS_ANALYTICS_MARKET_ANALYSIS_ENABLED")
	viper.BindEnv("market_analysis.time_window_minutes", "TS_ANALYTICS_MARKET_ANALYSIS_WINDOW")
	viper.BindEnv("market_analysis.confidence_threshold", "TS_ANALYTICS_MARKET_ANALYSIS_CONFIDENCE")

	if err := viper.ReadInConfig(); err != nil {
		// Ignore if config file not found
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	// Check required fields
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func validateConfig(cfg *Config) error {
	if cfg.InfluxDB.URL == "" {
		return fmt.Errorf("influxdb URL is required")
	}
	if cfg.InfluxDB.Token == "" {
		return fmt.Errorf("influxdb token is required")
	}
	if cfg.InfluxDB.Org == "" {
		return fmt.Errorf("influxdb organization is required")
	}
	if cfg.InfluxDB.Buckets.Candles == "" {
		return fmt.Errorf("influxdb candles bucket is required")
	}
	if cfg.InfluxDB.Buckets.OrderBook == "" {
		return fmt.Errorf("influxdb orderbook bucket is required")
	}
	if cfg.InfluxDB.Buckets.OrderBookAgg == "" {
		return fmt.Errorf("influxdb orderbook aggregation bucket is required")
	}
	
	// Set default market analysis bucket if not specified
	if cfg.InfluxDB.Buckets.MarketAnalysis == "" {
		cfg.InfluxDB.Buckets.MarketAnalysis = "market_analysis"
	}
	
	return nil
}
