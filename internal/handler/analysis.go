package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"

	"market-analytics-service/internal/analysis"
	"market-analytics-service/pkg/models"
)

// AnalysisHandler handles requests for market maker pattern analysis
type AnalysisHandler struct {
	manager *analysis.AnalysisManager
}

// NewAnalysisHandler creates a new handler for market maker pattern analysis
func NewAnalysisHandler(manager *analysis.AnalysisManager) *AnalysisHandler {
	return &AnalysisHandler{
		manager: manager,
	}
}

// Setup registers all HTTP routes for market maker pattern analysis
func (h *AnalysisHandler) Setup(router *mux.Router) {
	router.HandleFunc("/api/v1/analysis/status", h.GetAnalysisStatus).Methods("GET")
	router.HandleFunc("/api/v1/analysis/start", h.StartAnalysis).Methods("POST")
	router.HandleFunc("/api/v1/analysis/stop", h.StopAnalysis).Methods("POST")
	router.HandleFunc("/api/v1/analysis/run", h.RunSingleAnalysis).Methods("POST")
	router.HandleFunc("/api/v1/analysis/patterns", h.GetPatterns).Methods("GET")
}

// GetAnalysisStatus returns the current status of the market maker pattern analysis
func (h *AnalysisHandler) GetAnalysisStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"running": h.manager.IsRunning(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// StartAnalysis starts the market maker pattern analysis
func (h *AnalysisHandler) StartAnalysis(w http.ResponseWriter, r *http.Request) {
	if h.manager.IsRunning() {
		http.Error(w, "Analysis is already running", http.StatusConflict)
		return
	}

	if err := h.manager.Start(r.Context()); err != nil {
		http.Error(w, "Failed to start analysis: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Market maker pattern analysis started",
	})
}

// StopAnalysis stops the market maker pattern analysis
func (h *AnalysisHandler) StopAnalysis(w http.ResponseWriter, r *http.Request) {
	if !h.manager.IsRunning() {
		http.Error(w, "Analysis is not running", http.StatusConflict)
		return
	}

	h.manager.Stop()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Market maker pattern analysis stopped",
	})
}

// RunSingleAnalysis runs analysis on a specific market
type analysisRequest struct {
	Exchange    string `json:"exchange"`
	TradingPair string `json:"trading_pair"`
	ClientID    string `json:"client_id"`
}

// RunSingleAnalysis runs market maker pattern analysis on a specific market
func (h *AnalysisHandler) RunSingleAnalysis(w http.ResponseWriter, r *http.Request) {
	var req analysisRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Exchange == "" || req.TradingPair == "" {
		http.Error(w, "Exchange and trading pair are required", http.StatusBadRequest)
		return
	}

	log.Info().
		Str("exchange", req.Exchange).
		Str("tradingPair", req.TradingPair).
		Str("clientID", req.ClientID).
		Msg("Running manual market maker pattern analysis")

	result, err := h.manager.RunSingleAnalysis(r.Context(), req.Exchange, req.TradingPair, req.ClientID)
	if err != nil {
		http.Error(w, "Analysis failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// GetPatterns retrieves detected market maker patterns
func (h *AnalysisHandler) GetPatterns(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	exchange := r.URL.Query().Get("exchange")
	tradingPair := r.URL.Query().Get("trading_pair")
	patternType := r.URL.Query().Get("pattern_type")
	startStr := r.URL.Query().Get("start_time")
	endStr := r.URL.Query().Get("end_time")
	minConfidenceStr := r.URL.Query().Get("min_confidence")
	limitStr := r.URL.Query().Get("limit")

	// Set default values
	startTime := time.Now().Add(-24 * time.Hour)
	endTime := time.Now()
	minConfidence := 0.5
	limit := 100

	// Parse custom values if provided
	if startStr != "" {
		parsed, err := time.Parse(time.RFC3339, startStr)
		if err == nil {
			startTime = parsed
		}
	}

	if endStr != "" {
		parsed, err := time.Parse(time.RFC3339, endStr)
		if err == nil {
			endTime = parsed
		}
	}

	if minConfidenceStr != "" {
		parsed, err := strconv.ParseFloat(minConfidenceStr, 64)
		if err == nil {
			minConfidence = parsed
		}
	}

	if limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err == nil && parsed > 0 {
			limit = parsed
		}
	}

	// Build InfluxDB query
	query := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: %s, stop: %s)
			|> filter(fn: (r) => r["_measurement"] == "market_maker_patterns")`,
		"market_analysis",
		startTime.Format(time.RFC3339),
		endTime.Format(time.RFC3339))

	// Add optional filters
	if exchange != "" {
		query += fmt.Sprintf(`
			|> filter(fn: (r) => r["exchange"] == "%s")`, exchange)
	}

	if tradingPair != "" {
		query += fmt.Sprintf(`
			|> filter(fn: (r) => r["trading_pair"] == "%s")`, tradingPair)
	}

	if patternType != "" {
		query += fmt.Sprintf(`
			|> filter(fn: (r) => r["pattern_type"] == "%s")`, patternType)
	}

	// Add confidence filter and limit
	query += fmt.Sprintf(`
		|> filter(fn: (r) => r["confidence"] >= %f)
		|> sort(columns: ["_time"], desc: true)
		|> limit(n: %d)`,
		minConfidence, limit)

	// Get the patterns from the database
	patterns, err := h.getPatterns(r.Context(), query)
	if err != nil {
		http.Error(w, "Failed to retrieve patterns: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(patterns)
}

// getPatterns executes the query and returns the patterns
func (h *AnalysisHandler) getPatterns(ctx context.Context, query string) ([]*models.MarketMakerPattern, error) {
	// Get influxClient from the manager
	influxClient := h.manager.GetInfluxClient()
	if influxClient == nil {
		return nil, fmt.Errorf("influx client not available")
	}

	// Execute the query
	result, err := influxClient.GetQueryAPI().Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer result.Close()

	var patterns []*models.MarketMakerPattern

	// Process query results
	for result.Next() {
		record := result.Record()
		
		// Extract pattern data from the record
		pattern := &models.MarketMakerPattern{
			Exchange:    record.ValueByKey("exchange").(string),
			TradingPair: record.ValueByKey("trading_pair").(string),
			Timestamp:   record.Time(),
			PatternType: record.ValueByKey("pattern_type").(string),
			Confidence:  record.ValueByKey("confidence").(float64),
			Parameters:  make(map[string]interface{}),
		}

		// Extract all fields as parameters
		for k, v := range record.Values() {
			// Skip non-parameter fields
			if k == "_time" || k == "_measurement" || k == "exchange" || 
			   k == "trading_pair" || k == "pattern_type" || k == "_field" || 
			   k == "_value" || k == "confidence" || k == "_start" || k == "_stop" {
				continue
			}
			pattern.Parameters[k] = v
		}

		patterns = append(patterns, pattern)
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error processing query results: %w", err)
	}

	return patterns, nil
}