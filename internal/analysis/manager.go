package analysis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"market-analytics-service/internal/influx"
	"market-analytics-service/pkg/models"
)

// AnalysisManager coordinates market maker pattern analysis
type AnalysisManager struct {
	detector     *PatternDetector
	influxClient *influx.Client
	config       models.MMAnalysisConfig
	running      bool
	mu           sync.Mutex
	cancelFunc   context.CancelFunc
}

// NewAnalysisManager creates a new instance of the analysis manager
func NewAnalysisManager(influxClient *influx.Client, config models.MMAnalysisConfig) *AnalysisManager {
	// Use sensible defaults if not provided
	if config.MinOrderBookDepth <= 0 {
		config.MinOrderBookDepth = 10
	}
	if config.MinCandleCount <= 0 {
		config.MinCandleCount = 20
	}
	if config.ConfidenceThreshold <= 0 {
		config.ConfidenceThreshold = 0.7
	}
	if config.TimeWindowMinutes <= 0 {
		config.TimeWindowMinutes = 60
	}

	return &AnalysisManager{
		detector:     NewPatternDetector(influxClient, config),
		influxClient: influxClient,
		config:       config,
	}
}

// Start begins the analysis process for monitoring market maker patterns
func (m *AnalysisManager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("analysis manager is already running")
	}

	analysisCtx, cancel := context.WithCancel(ctx)
	m.cancelFunc = cancel
	m.running = true

	log.Info().
		Int("timeWindowMinutes", m.config.TimeWindowMinutes).
		Float64("confidenceThreshold", m.config.ConfidenceThreshold).
		Msg("Starting market maker pattern analysis manager")

	// Start the analysis goroutine
	go m.runAnalysis(analysisCtx)

	return nil
}

// Stop halts the analysis process
func (m *AnalysisManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return
	}

	if m.cancelFunc != nil {
		m.cancelFunc()
	}

	m.running = false
	log.Info().Msg("Stopped market maker pattern analysis manager")
}

// IsRunning returns the current state of the analysis manager
func (m *AnalysisManager) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

// runAnalysis executes the main analysis loop
func (m *AnalysisManager) runAnalysis(ctx context.Context) {
	// Get list of markets to analyze
	markets, err := m.getMarketsToAnalyze(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get markets for analysis")
		return
	}

	log.Info().
		Int("marketCount", len(markets)).
		Msg("Starting market maker pattern analysis for markets")

	// Create a ticker that runs every 15 minutes
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()

	// Run initial analysis
	m.analyzeAllMarkets(ctx, markets)

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Market maker pattern analysis stopped")
			return
		case <-ticker.C:
			// Refresh the list of markets periodically
			if len(markets) == 0 || time.Now().Minute()%30 == 0 {
				newMarkets, err := m.getMarketsToAnalyze(ctx)
				if err != nil {
					log.Error().Err(err).Msg("Failed to refresh markets for analysis")
				} else {
					markets = newMarkets
					log.Info().Int("marketCount", len(markets)).Msg("Refreshed market list for analysis")
				}
			}

			// Run analysis on all markets
			m.analyzeAllMarkets(ctx, markets)
		}
	}
}

// getMarketsToAnalyze retrieves the list of markets that should be analyzed
func (m *AnalysisManager) getMarketsToAnalyze(ctx context.Context) ([]MarketInfo, error) {
	// Query InfluxDB to get active markets with sufficient data
	query := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: -24h)
			|> filter(fn: (r) => r["_measurement"] == "candles" or r["_measurement"] == "orderbook")
			|> group(columns: ["exchange", "trading_pair"])
			|> count()
			|> filter(fn: (r) => r["_value"] > 100)
			|> keep(columns: ["exchange", "trading_pair"])
			|> distinct()
	`, m.influxClient.GetBucket("candles"))

	result, err := m.influxClient.GetQueryAPI().Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query active markets: %w", err)
	}
	defer result.Close()

	var markets []MarketInfo
	for result.Next() {
		record := result.Record()
		exchange := record.ValueByKey("exchange").(string)
		tradingPair := record.ValueByKey("trading_pair").(string)

		markets = append(markets, MarketInfo{
			Exchange:    exchange,
			TradingPair: tradingPair,
		})
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error processing market query results: %w", err)
	}

	return markets, nil
}

// analyzeAllMarkets runs analysis on all provided markets
func (m *AnalysisManager) analyzeAllMarkets(ctx context.Context, markets []MarketInfo) {
	if len(markets) == 0 {
		log.Warn().Msg("No markets to analyze")
		return
	}

	startTime := time.Now()
	log.Info().
		Int("marketCount", len(markets)).
		Msg("Starting analysis batch")

	// Create a worker pool to analyze markets concurrently
	workerCount := 5
	if len(markets) < workerCount {
		workerCount = len(markets)
	}

	// Create a channel for distributing work
	workChan := make(chan MarketInfo, len(markets))
	resultChan := make(chan *models.MMAnalysisResult, len(markets))
	errorChan := make(chan error, len(markets))

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for market := range workChan {
				analysisResult, err := m.detector.AnalyzeMarket(ctx, market.Exchange, market.TradingPair, "")
				if err != nil {
					errorChan <- fmt.Errorf("failed to analyze %s-%s: %w", market.Exchange, market.TradingPair, err)
					continue
				}
				resultChan <- analysisResult
			}
		}()
	}

	// Send markets to workers
	for _, market := range markets {
		select {
		case workChan <- market:
		case <-ctx.Done():
			close(workChan)
			return
		}
	}
	close(workChan)

	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	// Process results
	var results []*models.MMAnalysisResult
	for result := range resultChan {
		results = append(results, result)
	}

	// Log any errors
	for err := range errorChan {
		log.Error().Err(err).Msg("Error during market analysis")
	}

	// Save results to InfluxDB
	if len(results) > 0 {
		if err := m.saveResults(ctx, results); err != nil {
			log.Error().Err(err).Msg("Failed to save analysis results")
		}
	}

	log.Info().
		Int("analyzedMarkets", len(results)).
		Dur("duration", time.Since(startTime)).
		Msg("Completed market maker pattern analysis batch")
}

// saveResults stores the analysis results in InfluxDB
func (m *AnalysisManager) saveResults(ctx context.Context, results []*models.MMAnalysisResult) error {
	// Create a writer for InfluxDB
	writeAPI := m.influxClient.Client().WriteAPI(m.influxClient.GetOrg(), m.influxClient.GetBucket("market_analysis"))

	// Prepare points for each result
	for _, result := range results {
		// Save overall analysis metrics
		pointMetrics := influx.NewPoint(
			"market_maker_metrics",
			map[string]string{
				"exchange":     result.Exchange,
				"trading_pair": result.TradingPair,
			},
			result.MetricsSnapshot,
			result.AnalysisTime,
		)
		writeAPI.WritePoint(pointMetrics)

		// Save detected patterns
		for _, pattern := range result.DetectedPatterns {
			fields := map[string]interface{}{
				"confidence": pattern.Confidence,
			}

			// Add all parameters
			for key, value := range pattern.Parameters {
				fields[key] = value
			}

			pointPattern := influx.NewPoint(
				"market_maker_patterns",
				map[string]string{
					"exchange":     pattern.Exchange,
					"trading_pair": pattern.TradingPair,
					"pattern_type": pattern.PatternType,
				},
				fields,
				pattern.Timestamp,
			)
			writeAPI.WritePoint(pointPattern)
		}
	}

	// Flush writes
	writeAPI.Flush()
	
	return nil
}

// NewPoint creates a new InfluxDB point with the given fields
func (m *AnalysisManager) NewPoint(measurement string, tags map[string]string, fields map[string]interface{}, timestamp time.Time) influx.Point {
	return influx.NewPoint(measurement, tags, fields, timestamp)
}

// GetInfluxClient returns the InfluxDB client for use by handlers
func (m *AnalysisManager) GetInfluxClient() *influx.Client {
	return m.influxClient
}

// MarketInfo represents a trading market that can be analyzed
type MarketInfo struct {
	Exchange    string
	TradingPair string
}

// RunSingleAnalysis performs analysis on a specific market and returns the results
func (m *AnalysisManager) RunSingleAnalysis(ctx context.Context, exchange, tradingPair, clientID string) (*models.MMAnalysisResult, error) {
	log.Info().
		Str("exchange", exchange).
		Str("tradingPair", tradingPair).
		Str("clientID", clientID).
		Msg("Running single market analysis")

	result, err := m.detector.AnalyzeMarket(ctx, exchange, tradingPair, clientID)
	if err != nil {
		return nil, err
	}

	// Save the result to InfluxDB
	if err := m.saveResults(ctx, []*models.MMAnalysisResult{result}); err != nil {
		log.Error().
			Err(err).
			Str("exchange", exchange).
			Str("tradingPair", tradingPair).
			Msg("Failed to save analysis result")
	}

	return result, nil
}