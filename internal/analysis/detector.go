package analysis

import (
	"context"
	"math"
	"time"

	"github.com/rs/zerolog/log"

	"market-analytics-service/internal/influx"
	"market-analytics-service/pkg/models"
)

// PatternDetector is responsible for analyzing market data and detecting market maker patterns
type PatternDetector struct {
	influxClient *influx.Client
	config       models.MMAnalysisConfig
}

// NewPatternDetector creates a new instance of PatternDetector
func NewPatternDetector(influxClient *influx.Client, config models.MMAnalysisConfig) *PatternDetector {
	return &PatternDetector{
		influxClient: influxClient,
		config:       config,
	}
}

// AnalyzeMarket performs analysis on specified market data to detect market maker patterns
func (d *PatternDetector) AnalyzeMarket(ctx context.Context, exchange, tradingPair, clientID string) (*models.MMAnalysisResult, error) {
	log.Info().
		Str("exchange", exchange).
		Str("tradingPair", tradingPair).
		Str("clientID", clientID).
		Msg("Starting market maker pattern analysis")

	endTime := time.Now()
	startTime := endTime.Add(-time.Duration(d.config.TimeWindowMinutes) * time.Minute)

	// Get order book data
	orderBookParams := influx.QueryParams{
		StartTime:   startTime,
		EndTime:     endTime,
		ClientID:    clientID,
		Exchange:    exchange,
		TradingPair: tradingPair,
		Bucket:      d.influxClient.GetBucket("orderbook"),
	}

	orderBookData, err := d.influxClient.GetOrderBookData(ctx, orderBookParams)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get order book data for pattern analysis")
		return nil, err
	}

	// Get candle data
	candleParams := influx.QueryParams{
		StartTime:   startTime,
		EndTime:     endTime,
		ClientID:    clientID,
		Exchange:    exchange,
		TradingPair: tradingPair,
		Bucket:      d.influxClient.GetBucket("candles"),
		Interval:    "1m", // Using 1-minute candles for analysis
	}

	candleData, err := d.influxClient.GetHistoricalData(ctx, candleParams)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get candle data for pattern analysis")
		return nil, err
	}

	// Check if we have enough data
	if len(orderBookData) < d.config.MinOrderBookDepth || len(candleData) < d.config.MinCandleCount {
		log.Warn().
			Int("orderBookDataCount", len(orderBookData)).
			Int("candleDataCount", len(candleData)).
			Int("requiredOrderBookDepth", d.config.MinOrderBookDepth).
			Int("requiredCandleCount", d.config.MinCandleCount).
			Msg("Insufficient data for pattern analysis")
		
		return &models.MMAnalysisResult{
			Exchange:         exchange,
			TradingPair:      tradingPair,
			AnalysisTime:     time.Now(),
			WindowStart:      startTime,
			WindowEnd:        endTime,
			DetectedPatterns: []*models.MarketMakerPattern{},
			MetricsSnapshot:  map[string]float64{
				"data_sufficiency": 0.0,
			},
		}, nil
	}

	// Initialize result
	result := &models.MMAnalysisResult{
		Exchange:         exchange,
		TradingPair:      tradingPair,
		AnalysisTime:     time.Now(),
		WindowStart:      startTime,
		WindowEnd:        endTime,
		DetectedPatterns: []*models.MarketMakerPattern{},
		MetricsSnapshot:  make(map[string]float64),
	}

	// Calculate basic metrics for the analysis window
	result.MetricsSnapshot = d.calculateBaseMetrics(candleData, orderBookData)
	
	// Detect patterns
	d.detectSpoofingPattern(orderBookData, result)
	d.detectLayeringPattern(orderBookData, result)
	d.detectMomentumIgnition(candleData, orderBookData, result)
	d.detectIcebergOrders(orderBookData, result)
	d.detectTwapVwapPatterns(candleData, result)
	d.detectStopHunting(candleData, result)
	
	// Log analysis results
	log.Info().
		Str("exchange", exchange).
		Str("tradingPair", tradingPair).
		Int("detectedPatterns", len(result.DetectedPatterns)).
		Float64("priceVolatility", result.MetricsSnapshot["price_volatility"]).
		Msg("Market maker pattern analysis completed")
		
	return result, nil
}

// Calculate base metrics needed for pattern detection
func (d *PatternDetector) calculateBaseMetrics(candles, orderBook []models.MarketData) map[string]float64 {
	metrics := make(map[string]float64)
	
	// Calculate price volatility from candles
	if len(candles) > 1 {
		var priceChanges []float64
		var lastClose float64
		
		for i, candle := range candles {
			if data, ok := candle.Data.(map[string]interface{}); ok {
				close := getFloat(data["close"])
				if i > 0 && lastClose > 0 {
					priceChange := math.Abs(close - lastClose) / lastClose
					priceChanges = append(priceChanges, priceChange)
				}
				lastClose = close
			}
		}
		
		metrics["price_volatility"] = calculateStdDev(priceChanges)
	}
	
	// Calculate bid-ask spread and depth from order book
	if len(orderBook) > 0 {
		var bidPrices []float64
		var askPrices []float64
		var bidVolumes []float64
		var askVolumes []float64
		
		for _, entry := range orderBook {
			if data, ok := entry.Data.(map[string]interface{}); ok {
				side := getString(data["side"])
				price := getFloat(data["price"])
				volume := getFloat(data["volume"])
				
				if side == "bid" {
					bidPrices = append(bidPrices, price)
					bidVolumes = append(bidVolumes, volume)
				} else if side == "ask" {
					askPrices = append(askPrices, price)
					askVolumes = append(askVolumes, volume)
				}
			}
		}
		
		// Calculate metrics if we have both bids and asks
		if len(bidPrices) > 0 && len(askPrices) > 0 {
			maxBid := findMax(bidPrices)
			minAsk := findMin(askPrices)
			
			if maxBid > 0 && minAsk > 0 {
				spread := minAsk - maxBid
				midPrice := (minAsk + maxBid) / 2
				metrics["bid_ask_spread"] = spread
				metrics["spread_percentage"] = spread / midPrice * 100
				
				// Calculate depth at different levels
				metrics["bid_volume_total"] = sumArray(bidVolumes)
				metrics["ask_volume_total"] = sumArray(askVolumes)
				metrics["volume_imbalance"] = (metrics["bid_volume_total"] - metrics["ask_volume_total"]) / 
					(metrics["bid_volume_total"] + metrics["ask_volume_total"])
			}
		}
	}
	
	// Calculate data sufficiency score
	metrics["data_sufficiency"] = calculateDataSufficiency(candles, orderBook, d.config)
	
	return metrics
}

// Detect spoofing patterns (large orders that are cancelled quickly)
func (d *PatternDetector) detectSpoofingPattern(orderBook []models.MarketData, result *models.MMAnalysisResult) {
	// Group order book entries by price level to track additions and removals
	priceMap := make(map[string][]models.MarketData)
	
	for _, entry := range orderBook {
		if data, ok := entry.Data.(map[string]interface{}); ok {
			price := getFloat(data["price"])
			priceKey := formatFloat(price, 8)
			priceMap[priceKey] = append(priceMap[priceKey], entry)
		}
	}
	
	// Look for large orders that appear and disappear quickly
	var spoofingInstances int
	var largeOrdersCount int
	var largeOrderVolume float64
	var averageLifetime float64
	
	for _, entries := range priceMap {
		if len(entries) < 2 {
			continue // Need at least 2 entries to detect appearance and disappearance
		}
		
		// Sort by timestamp
		sortByTimestamp(entries)
		
		for i := 0; i < len(entries)-1; i++ {
			currentEntry := entries[i]
			nextEntry := entries[i+1]
			
			currentData := currentEntry.Data.(map[string]interface{})
			nextData := nextEntry.Data.(map[string]interface{})
			
			currentVolume := getFloat(currentData["volume"])
			nextVolume := getFloat(nextData["volume"])
			
			// If volume significantly decreased or disappeared
			if currentVolume > nextVolume*3 && currentVolume > result.MetricsSnapshot["bid_volume_total"]*0.1 {
				lifetime := nextEntry.Timestamp.Sub(currentEntry.Timestamp)
				
				// If order was short-lived (less than 30 seconds)
				if lifetime < 30*time.Second {
					spoofingInstances++
					largeOrdersCount++
					largeOrderVolume += currentVolume
					averageLifetime += lifetime.Seconds()
				}
			}
		}
	}
	
	if spoofingInstances > 0 {
		averageLifetime /= float64(spoofingInstances)
		confidence := calculateSpoofingConfidence(spoofingInstances, largeOrderVolume, 
			result.MetricsSnapshot["bid_volume_total"]+result.MetricsSnapshot["ask_volume_total"])
		
		if confidence >= d.config.ConfidenceThreshold {
			result.DetectedPatterns = append(result.DetectedPatterns, &models.MarketMakerPattern{
				Exchange:    result.Exchange,
				TradingPair: result.TradingPair,
				Timestamp:   time.Now(),
				PatternType: models.PatternSpoofing,
				Confidence:  confidence,
				Parameters: map[string]interface{}{
					"instances":        spoofingInstances,
					"large_orders":     largeOrdersCount,
					"avg_lifetime_sec": averageLifetime,
					"volume_ratio":     largeOrderVolume / (result.MetricsSnapshot["bid_volume_total"] + result.MetricsSnapshot["ask_volume_total"]),
				},
			})
		}
	}
}

// Detect layering patterns (multiple orders at different price levels)
func (d *PatternDetector) detectLayeringPattern(orderBook []models.MarketData, result *models.MMAnalysisResult) {
	// Group by timestamp to analyze order book snapshots
	timestampMap := make(map[time.Time][]models.MarketData)
	var timestamps []time.Time
	
	for _, entry := range orderBook {
		timestamp := entry.Timestamp.Truncate(time.Second)
		timestampMap[timestamp] = append(timestampMap[timestamp], entry)
		if len(timestampMap[timestamp]) == 1 {
			timestamps = append(timestamps, timestamp)
		}
	}
	
	// Sort timestamps
	sortTimestamps(timestamps)
	
	var layeringInstances int
	var totalLayerCount int
	var avgLayerSize float64
	
	// Analyze each snapshot
	for _, timestamp := range timestamps {
		entries := timestampMap[timestamp]
		
		// Count bid and ask layers
		bidPrices := make(map[string]float64)
		askPrices := make(map[string]float64)
		
		for _, entry := range entries {
			if data, ok := entry.Data.(map[string]interface{}); ok {
				side := getString(data["side"])
				price := getFloat(data["price"])
				volume := getFloat(data["volume"])
				priceKey := formatFloat(price, 8)
				
				if side == "bid" {
					bidPrices[priceKey] = volume
				} else if side == "ask" {
					askPrices[priceKey] = volume
				}
			}
		}
		
		// Check for layering pattern (multiple small orders at different price levels)
		if len(bidPrices) > 5 || len(askPrices) > 5 {
			// Calculate average order size
			var totalBidVolume, totalAskVolume float64
			
			for _, volume := range bidPrices {
				totalBidVolume += volume
			}
			
			for _, volume := range askPrices {
				totalAskVolume += volume
			}
			
			avgBidSize := totalBidVolume / float64(len(bidPrices))
			avgAskSize := totalAskVolume / float64(len(askPrices))
			
			// Detect if many small orders are present
			if (len(bidPrices) > 5 && avgBidSize < result.MetricsSnapshot["bid_volume_total"]/float64(len(bidPrices))/2) ||
				(len(askPrices) > 5 && avgAskSize < result.MetricsSnapshot["ask_volume_total"]/float64(len(askPrices))/2) {
				
				layeringInstances++
				totalLayerCount += len(bidPrices) + len(askPrices)
				avgLayerSize += (avgBidSize + avgAskSize) / 2
			}
		}
	}
	
	if layeringInstances > 0 {
		avgLayerSize /= float64(layeringInstances)
		avgLayerCount := float64(totalLayerCount) / float64(layeringInstances)
		
		confidence := calculateLayeringConfidence(layeringInstances, avgLayerCount, len(timestamps))
		
		if confidence >= d.config.ConfidenceThreshold {
			result.DetectedPatterns = append(result.DetectedPatterns, &models.MarketMakerPattern{
				Exchange:    result.Exchange,
				TradingPair: result.TradingPair,
				Timestamp:   time.Now(),
				PatternType: models.PatternLayering,
				Confidence:  confidence,
				Parameters: map[string]interface{}{
					"instances":       layeringInstances,
					"avg_layer_count": avgLayerCount,
					"avg_layer_size":  avgLayerSize,
				},
			})
		}
	}
}

// Detect momentum ignition (price manipulation by triggering stop orders)
func (d *PatternDetector) detectMomentumIgnition(candles, orderBook []models.MarketData, result *models.MMAnalysisResult) {
	if len(candles) < 5 {
		return
	}
	
	// Look for sudden price movements followed by reversals
	var priceSpikes []float64
	var volumeSpikes []float64
	var reversals int
	
	for i := 1; i < len(candles)-1; i++ {
		prevCandle := candles[i-1]
		currCandle := candles[i]
		nextCandle := candles[i+1]
		
		prevData := prevCandle.Data.(map[string]interface{})
		currData := currCandle.Data.(map[string]interface{})
		nextData := nextCandle.Data.(map[string]interface{})
		
		prevClose := getFloat(prevData["close"])
		currOpen := getFloat(currData["open"])
		currClose := getFloat(currData["close"])
		currHigh := getFloat(currData["high"])
		currLow := getFloat(currData["low"])
		currVolume := getFloat(currData["volume"])
		nextOpen := getFloat(nextData["open"])
		nextClose := getFloat(nextData["close"])
		
		// Calculate price movement
		priceChange := math.Abs(currClose - prevClose) / prevClose
		
		// Check for price spike
		if priceChange > result.MetricsSnapshot["price_volatility"]*2 {
			priceSpikes = append(priceSpikes, priceChange)
			volumeSpikes = append(volumeSpikes, currVolume)
			
			// Check for reversal
			if (currClose > prevClose && nextClose < currClose) || 
				(currClose < prevClose && nextClose > currClose) {
				reversals++
			}
		}
	}
	
	if len(priceSpikes) > 0 {
		avgPriceSpike := sumArray(priceSpikes) / float64(len(priceSpikes))
		avgVolumeSpike := sumArray(volumeSpikes) / float64(len(volumeSpikes))
		reversalRatio := float64(reversals) / float64(len(priceSpikes))
		
		confidence := calculateMomentumIgnitionConfidence(
			len(priceSpikes), 
			avgPriceSpike, 
			result.MetricsSnapshot["price_volatility"],
			reversalRatio,
		)
		
		if confidence >= d.config.ConfidenceThreshold {
			result.DetectedPatterns = append(result.DetectedPatterns, &models.MarketMakerPattern{
				Exchange:    result.Exchange,
				TradingPair: result.TradingPair,
				Timestamp:   time.Now(),
				PatternType: models.PatternMomentumIgnition,
				Confidence:  confidence,
				Parameters: map[string]interface{}{
					"price_spikes":    len(priceSpikes),
					"avg_price_spike": avgPriceSpike,
					"avg_volume_spike": avgVolumeSpike,
					"reversal_ratio":  reversalRatio,
				},
			})
		}
	}
}

// Detect iceberg orders (large orders broken into smaller pieces)
func (d *PatternDetector) detectIcebergOrders(orderBook []models.MarketData, result *models.MMAnalysisResult) {
	// Group by timestamp to analyze order book snapshots
	timestampMap := make(map[time.Time][]models.MarketData)
	var timestamps []time.Time
	
	for _, entry := range orderBook {
		timestamp := entry.Timestamp.Truncate(time.Second)
		timestampMap[timestamp] = append(timestampMap[timestamp], entry)
		if len(timestampMap[timestamp]) == 1 {
			timestamps = append(timestamps, timestamp)
		}
	}
	
	// Sort timestamps
	sortTimestamps(timestamps)
	
	// Track order sizes at same price levels across snapshots
	priceHistory := make(map[string][]float64)
	
	for _, timestamp := range timestamps {
		entries := timestampMap[timestamp]
		
		for _, entry := range entries {
			if data, ok := entry.Data.(map[string]interface{}); ok {
				side := getString(data["side"])
				price := getFloat(data["price"])
				volume := getFloat(data["volume"])
				priceKey := side + "_" + formatFloat(price, 8)
				
				priceHistory[priceKey] = append(priceHistory[priceKey], volume)
			}
		}
	}
	
	// Look for recurring order sizes at same price level
	var icebergCount int
	var repeatCount int
	var sizeVariance float64
	
	for priceKey, volumes := range priceHistory {
		if len(volumes) < 3 {
			continue
		}
		
		// Calculate variance and check for repeating patterns
		variance := calculateVariance(volumes)
		
		// Low variance indicates similar order sizes being repeatedly placed
		if variance < 0.1 && len(volumes) > 5 {
			icebergCount++
			repeatCount += len(volumes)
			sizeVariance += variance
		}
	}
	
	if icebergCount > 0 {
		avgRepeatCount := float64(repeatCount) / float64(icebergCount)
		avgSizeVariance := sizeVariance / float64(icebergCount)
		
		confidence := calculateIcebergConfidence(icebergCount, avgRepeatCount, avgSizeVariance)
		
		if confidence >= d.config.ConfidenceThreshold {
			result.DetectedPatterns = append(result.DetectedPatterns, &models.MarketMakerPattern{
				Exchange:    result.Exchange,
				TradingPair: result.TradingPair,
				Timestamp:   time.Now(),
				PatternType: models.PatternIceberg,
				Confidence:  confidence,
				Parameters: map[string]interface{}{
					"iceberg_levels":  icebergCount,
					"avg_repeat":      avgRepeatCount,
					"size_variance":   avgSizeVariance,
				},
			})
		}
	}
}

// Detect TWAP/VWAP execution patterns
func (d *PatternDetector) detectTwapVwapPatterns(candles []models.MarketData, result *models.MMAnalysisResult) {
	if len(candles) < 10 {
		return
	}
	
	// Calculate trade distribution over time
	var volumes []float64
	var timestamps []time.Time
	
	for _, candle := range candles {
		if data, ok := candle.Data.(map[string]interface{}); ok {
			volume := getFloat(data["volume"])
			volumes = append(volumes, volume)
			timestamps = append(timestamps, candle.Timestamp)
		}
	}
	
	// Calculate time-weighted volume
	timeInterval := timestamps[len(timestamps)-1].Sub(timestamps[0]).Minutes()
	volumePerMinute := sumArray(volumes) / timeInterval
	
	// Calculate volume variance across time segments
	timeSegments := 5
	if len(candles) >= 20 {
		timeSegments = 10
	}
	
	segmentVolumes := make([]float64, timeSegments)
	segmentCount := make([]int, timeSegments)
	
	for i, candle := range candles {
		if data, ok := candle.Data.(map[string]interface{}); ok {
			// Determine which segment this candle belongs to
			segment := int(float64(i) / float64(len(candles)) * float64(timeSegments))
			if segment >= timeSegments {
				segment = timeSegments - 1
			}
			
			volume := getFloat(data["volume"])
			segmentVolumes[segment] += volume
			segmentCount[segment]++
		}
	}
	
	// Calculate coefficient of variation for volume distribution
	var segmentAvgs []float64
	for i := 0; i < timeSegments; i++ {
		if segmentCount[i] > 0 {
			segmentAvgs = append(segmentAvgs, segmentVolumes[i]/float64(segmentCount[i]))
		}
	}
	
	volumeCV := calculateCoefficientOfVariation(segmentAvgs)
	
	// TWAP pattern: consistent volume over time
	if volumeCV < 0.3 {
		confidence := calculateTwapConfidence(volumeCV, len(candles))
		
		if confidence >= d.config.ConfidenceThreshold {
			result.DetectedPatterns = append(result.DetectedPatterns, &models.MarketMakerPattern{
				Exchange:    result.Exchange,
				TradingPair: result.TradingPair,
				Timestamp:   time.Now(),
				PatternType: models.PatternTwap,
				Confidence:  confidence,
				Parameters: map[string]interface{}{
					"volume_cv":         volumeCV,
					"volume_per_minute": volumePerMinute,
					"segments":          timeSegments,
				},
			})
		}
	}
	
	// VWAP pattern: volume weighted by price
	priceVolumeCorrelation := calculatePriceVolumeCorrelation(candles)
	if math.Abs(priceVolumeCorrelation) > 0.6 {
		confidence := calculateVwapConfidence(priceVolumeCorrelation, len(candles))
		
		if confidence >= d.config.ConfidenceThreshold {
			result.DetectedPatterns = append(result.DetectedPatterns, &models.MarketMakerPattern{
				Exchange:    result.Exchange,
				TradingPair: result.TradingPair,
				Timestamp:   time.Now(),
				PatternType: models.PatternVwap,
				Confidence:  confidence,
				Parameters: map[string]interface{}{
					"price_volume_correlation": priceVolumeCorrelation,
					"candle_count":             len(candles),
				},
			})
		}
	}
}

// Detect stop hunting patterns
func (d *PatternDetector) detectStopHunting(candles []models.MarketData, result *models.MMAnalysisResult) {
	if len(candles) < 10 {
		return
	}
	
	var wickRatios []float64
	var priceReversals int
	var wicksDown int
	var wicksUp int
	
	for i := 1; i < len(candles)-1; i++ {
		prevCandle := candles[i-1]
		currCandle := candles[i]
		nextCandle := candles[i+1]
		
		prevData := prevCandle.Data.(map[string]interface{})
		currData := currCandle.Data.(map[string]interface{})
		nextData := nextCandle.Data.(map[string]interface{})
		
		currOpen := getFloat(currData["open"])
		currClose := getFloat(currData["close"])
		currHigh := getFloat(currData["high"])
		currLow := getFloat(currData["low"])
		nextOpen := getFloat(nextData["open"])
		
		bodySize := math.Abs(currClose - currOpen)
		
		// Calculate upper and lower wick sizes
		upperWick := currHigh - math.Max(currOpen, currClose)
		lowerWick := math.Min(currOpen, currClose) - currLow
		
		// Calculate wick ratio (wick size relative to body)
		if bodySize > 0 {
			upperWickRatio := upperWick / bodySize
			lowerWickRatio := lowerWick / bodySize
			
			// Long wicks can indicate stop hunting
			if upperWickRatio > 1.0 {
				wickRatios = append(wickRatios, upperWickRatio)
				wicksUp++
			}
			
			if lowerWickRatio > 1.0 {
				wickRatios = append(wickRatios, lowerWickRatio)
				wicksDown++
			}
			
			// Check for price reversal after a long wick
			if (upperWickRatio > 1.0 && nextOpen < currClose) || 
				(lowerWickRatio > 1.0 && nextOpen > currClose) {
				priceReversals++
			}
		}
	}
	
	if len(wickRatios) > 0 {
		avgWickRatio := sumArray(wickRatios) / float64(len(wickRatios))
		reversalRatio := float64(priceReversals) / float64(len(wickRatios))
		
		// Higher confidence if there are many wicks with reversals
		confidence := calculateStopHuntingConfidence(len(wickRatios), avgWickRatio, reversalRatio)
		
		if confidence >= d.config.ConfidenceThreshold {
			result.DetectedPatterns = append(result.DetectedPatterns, &models.MarketMakerPattern{
				Exchange:    result.Exchange,
				TradingPair: result.TradingPair,
				Timestamp:   time.Now(),
				PatternType: models.PatternStop,
				Confidence:  confidence,
				Parameters: map[string]interface{}{
					"wicks_count":     len(wickRatios),
					"wicks_up":        wicksUp,
					"wicks_down":      wicksDown,
					"avg_wick_ratio":  avgWickRatio,
					"reversal_ratio":  reversalRatio,
				},
			})
		}
	}
}

// Helper functions

func getFloat(val interface{}) float64 {
	if val == nil {
		return 0
	}
	switch v := val.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		f, _ := parseFloat(v)
		return f
	}
	return 0
}

func getString(val interface{}) string {
	if val == nil {
		return ""
	}
	switch v := val.(type) {
	case string:
		return v
	}
	return ""
}

func parseFloat(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	return 0, nil // In a real implementation, this would parse the string
}

func formatFloat(val float64, precision int) string {
	return fmt.Sprintf("%."+strconv.Itoa(precision)+"f", val)
}

func calculateStdDev(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	
	mean := sumArray(values) / float64(len(values))
	var variance float64
	
	for _, v := range values {
		variance += math.Pow(v-mean, 2)
	}
	
	return math.Sqrt(variance / float64(len(values)))
}

func calculateVariance(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	
	mean := sumArray(values) / float64(len(values))
	var variance float64
	
	for _, v := range values {
		variance += math.Pow(v-mean, 2)
	}
	
	return variance / float64(len(values))
}

func calculateCoefficientOfVariation(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	
	stdDev := calculateStdDev(values)
	mean := sumArray(values) / float64(len(values))
	
	if mean == 0 {
		return 0
	}
	
	return stdDev / mean
}

func calculatePriceVolumeCorrelation(candles []models.MarketData) float64 {
	if len(candles) < 3 {
		return 0
	}
	
	var prices []float64
	var volumes []float64
	
	for _, candle := range candles {
		if data, ok := candle.Data.(map[string]interface{}); ok {
			close := getFloat(data["close"])
			volume := getFloat(data["volume"])
			
			prices = append(prices, close)
			volumes = append(volumes, volume)
		}
	}
	
	return calculateCorrelation(prices, volumes)
}

func calculateCorrelation(x, y []float64) float64 {
	if len(x) != len(y) || len(x) == 0 {
		return 0
	}
	
	n := float64(len(x))
	var sumX, sumY, sumXY, sumXSquared, sumYSquared float64
	
	for i := range x {
		sumX += x[i]
		sumY += y[i]
		sumXY += x[i] * y[i]
		sumXSquared += x[i] * x[i]
		sumYSquared += y[i] * y[i]
	}
	
	numerator := sumXY - (sumX * sumY / n)
	denominator := math.Sqrt((sumXSquared - (sumX * sumX / n)) * (sumYSquared - (sumY * sumY / n)))
	
	if denominator == 0 {
		return 0
	}
	
	return numerator / denominator
}

func sumArray(arr []float64) float64 {
	var sum float64
	for _, v := range arr {
		sum += v
	}
	return sum
}

func findMax(arr []float64) float64 {
	if len(arr) == 0 {
		return 0
	}
	
	max := arr[0]
	for _, v := range arr {
		if v > max {
			max = v
		}
	}
	return max
}

func findMin(arr []float64) float64 {
	if len(arr) == 0 {
		return 0
	}
	
	min := arr[0]
	for _, v := range arr {
		if v < min {
			min = v
		}
	}
	return min
}

func sortByTimestamp(entries []models.MarketData) {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Timestamp.Before(entries[j].Timestamp)
	})
}

func sortTimestamps(timestamps []time.Time) {
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Before(timestamps[j])
	})
}

// Confidence calculation functions

func calculateSpoofingConfidence(instances int, volume, totalVolume float64) float64 {
	if totalVolume == 0 {
		return 0
	}
	
	volumeRatio := volume / totalVolume
	instanceFactor := math.Min(float64(instances)/10.0, 1.0)
	
	return math.Min(volumeRatio*1.5+instanceFactor*0.5, 1.0)
}

func calculateLayeringConfidence(instances int, avgLayerCount float64, totalSnapshots int) float64 {
	if totalSnapshots == 0 {
		return 0
	}
	
	instanceRatio := float64(instances) / float64(totalSnapshots)
	layerFactor := math.Min(avgLayerCount/15.0, 1.0)
	
	return math.Min(instanceRatio*0.7+layerFactor*0.3, 1.0)
}

func calculateMomentumIgnitionConfidence(spikeCount int, avgSpike, volatility, reversalRatio float64) float64 {
	spikeFactor := math.Min(float64(spikeCount)/5.0, 1.0)
	volatilityRatio := math.Min(avgSpike/volatility, 5.0) / 5.0
	
	return math.Min(spikeFactor*0.3+volatilityRatio*0.3+reversalRatio*0.4, 1.0)
}

func calculateIcebergConfidence(levels int, avgRepeat, variance float64) float64 {
	levelFactor := math.Min(float64(levels)/5.0, 1.0)
	repeatFactor := math.Min(avgRepeat/10.0, 1.0)
	varianceFactor := math.Max(1.0-variance*10.0, 0.0)
	
	return math.Min(levelFactor*0.3+repeatFactor*0.3+varianceFactor*0.4, 1.0)
}

func calculateTwapConfidence(volumeCV float64, candleCount int) float64 {
	cvFactor := math.Max(1.0-volumeCV*2.0, 0.0)
	countFactor := math.Min(float64(candleCount)/30.0, 1.0)
	
	return math.Min(cvFactor*0.7+countFactor*0.3, 1.0)
}

func calculateVwapConfidence(correlation float64, candleCount int) float64 {
	correlationFactor := math.Abs(correlation)
	countFactor := math.Min(float64(candleCount)/30.0, 1.0)
	
	return math.Min(correlationFactor*0.7+countFactor*0.3, 1.0)
}

func calculateStopHuntingConfidence(wickCount int, avgRatio, reversalRatio float64) float64 {
	countFactor := math.Min(float64(wickCount)/5.0, 1.0)
	ratioFactor := math.Min(avgRatio/3.0, 1.0)
	
	return math.Min(countFactor*0.3+ratioFactor*0.3+reversalRatio*0.4, 1.0)
}

func calculateDataSufficiency(candles, orderBook []models.MarketData, config models.MMAnalysisConfig) float64 {
	candleRatio := float64(len(candles)) / float64(config.MinCandleCount)
	orderBookRatio := float64(len(orderBook)) / float64(config.MinOrderBookDepth)
	
	return math.Min(math.Min(candleRatio, orderBookRatio), 1.0)
}