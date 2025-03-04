package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"market-analytics-service/internal/service"

	"github.com/gorilla/mux"
)

type HTTPHandler struct {
	analytics *service.AnalyticsService
}

func NewHTTPHandler(analytics *service.AnalyticsService) *HTTPHandler {
	return &HTTPHandler{
		analytics: analytics,
	}
}

// Setup registers all HTTP routes
func (h *HTTPHandler) Setup(router *mux.Router) {
	router.HandleFunc("/api/v1/historical", h.GetHistoricalData).Methods("GET")
}

// GetHistoricalData handles requests for historical market data
func (h *HTTPHandler) GetHistoricalData(w http.ResponseWriter, r *http.Request) {
	// Extract client_id from header
	clientID := r.Header.Get("client_id")

	// Parse query parameters
	startStr := r.URL.Query().Get("start_time")
	endStr := r.URL.Query().Get("end_time")

	if startStr == "" || endStr == "" {
		http.Error(w, "start_time and end_time are required", http.StatusBadRequest)
		return
	}

	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		http.Error(w, "invalid start_time format, use RFC3339", http.StatusBadRequest)
		return
	}

	endTime, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		http.Error(w, "invalid end_time format, use RFC3339", http.StatusBadRequest)
		return
	}

	// Get historical data
	data, err := h.analytics.GetHistoricalData(r.Context(), clientID, startTime, endTime)
	if err != nil {
		http.Error(w, "failed to get historical data: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Send response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)
		return
	}
}
