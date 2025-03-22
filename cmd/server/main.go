package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"market-analytics-service/internal/config"
	"market-analytics-service/internal/handler"
	"market-analytics-service/internal/influx"
	"market-analytics-service/internal/postgres"
	"market-analytics-service/internal/service"
)

func main() {
	// Initialize logger
	setupLogger()
	log.Info().Msg("Starting analytics service...")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// Create InfluxDB client
	influxClient, err := influx.NewClient(
		cfg.InfluxDB.URL,
		cfg.InfluxDB.Token,
		cfg.InfluxDB.Org,
		cfg.InfluxDB.Bucket,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create InfluxDB client")
	}
	defer influxClient.Close()

	// Initialize PostgreSQL client
	postgresClient, err := postgres.NewClient(postgres.Config{
		Host:     cfg.Postgres.Host,
		Port:     cfg.Postgres.Port,
		User:     cfg.Postgres.User,
		Password: cfg.Postgres.Password,
		DBName:   cfg.Postgres.DBName,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create PostgreSQL client")
	}
	defer postgresClient.Close()

	// Create analytics service
	analyticsService := service.NewAnalyticsService(influxClient, postgresClient)
	defer analyticsService.Close()

	// Create handlers
	httpHandler := handler.NewHTTPHandler(analyticsService)
	wsHandler := handler.NewWSHandler(analyticsService)

	// Setup router
	router := mux.NewRouter()

	// HTTP routes
	httpHandler.Setup(router)

	// WebSocket route
	router.HandleFunc("/ws", wsHandler.HandleWS)

	// Middleware for all routes
	router.Use(loggingMiddleware)
	router.Use(authMiddleware)
	router.Use(corsMiddleware)

	// Create HTTP server
	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.HTTPPort),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		log.Info().
			Str("addr", srv.Addr).
			Msg("Starting HTTP server")

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("Server failed")
		}
	}()

	// Wait for interrupt signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Info().Msg("Shutting down server...")

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Shutdown server gracefully
	if err := srv.Shutdown(ctx); err != nil {
		log.Error().Err(err).Msg("Server forced to shutdown")
	}

	log.Info().Msg("Server stopped")
}

func setupLogger() {
	// Set up zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.With().Caller().Logger()

	// Set log level from environment
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	level, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	// Use pretty logging for development
	if os.Getenv("LOG_PRETTY") == "true" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}
}

// Middleware to check and validate user ID
func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientID := r.Header.Get("X-User-ID")
		if clientID == "" {
			http.Error(w, "X-User-ID header is required", http.StatusUnauthorized)
			return
		}

		// Store clientID in context for later use
		ctx := context.WithValue(r.Context(), "clientID", clientID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		clientID := r.Header.Get("X-User-ID") // Updated from client_id to X-User-ID

		// Don't wrap WebSocket connections
		if r.Header.Get("Upgrade") == "websocket" {
			next.ServeHTTP(w, r)
			log.Info().
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Str("clientID", clientID).
				Dur("duration", time.Since(start)).
				Msg("WebSocket connection handled")
			return
		}

		// For non-WebSocket requests, use wrapped response writer
		wrapped := wrapResponseWriter(w)
		next.ServeHTTP(wrapped, r)

		log.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Str("clientID", clientID).
			Int("status", wrapped.status).
			Dur("duration", time.Since(start)).
			Msg("Request processed")
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-User-ID")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// responseWriter is a custom response writer that captures the status code
type responseWriter struct {
	http.ResponseWriter
	status int
}

func wrapResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w}
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}
