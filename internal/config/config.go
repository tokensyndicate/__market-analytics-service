package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	InfluxDB InfluxDBConfig `mapstructure:"influxdb"`
	Postgres PostgresConfig `mapstructure:"postgres"`
}

type ServerConfig struct {
	HTTPPort int    `mapstructure:"http_port"`
	WSPort   int    `mapstructure:"ws_port"`
	Host     string `mapstructure:"host"`
}

type InfluxDBConfig struct {
	URL    string `mapstructure:"url"`
	Token  string `mapstructure:"token"`
	Org    string `mapstructure:"org"`
	Bucket string `mapstructure:"bucket"`
}

type PostgresConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	DBName   string `mapstructure:"dbname"`
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

	// Postgres env bindings
	viper.BindEnv("postgres.host", "TS_ANALYTICS_POSTGRES_HOST")
	viper.BindEnv("postgres.port", "TS_ANALYTICS_POSTGRES_PORT")
	viper.BindEnv("postgres.user", "TS_ANALYTICS_POSTGRES_USER")
	viper.BindEnv("postgres.password", "TS_ANALYTICS_POSTGRES_PASSWORD")
	viper.BindEnv("postgres.dbname", "TS_ANALYTICS_POSTGRES_DBNAME")

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
	if cfg.InfluxDB.Bucket == "" {
		return fmt.Errorf("influxdb bucket is required")
	}

	// Validate Postgres configuration
	if cfg.Postgres.User == "" {
		return fmt.Errorf("postgres user is required")
	}
	if cfg.Postgres.DBName == "" {
		return fmt.Errorf("postgres database name is required")
	}

	return nil
}
