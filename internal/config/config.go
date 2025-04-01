package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	InfluxDB InfluxDBConfig `mapstructure:"influxdb"`
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
	Candles      string `mapstructure:"candles"`
	OrderBook    string `mapstructure:"orderbook"`
	OrderBookAgg string `mapstructure:"orderbook_agg"`
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
	return nil
}
