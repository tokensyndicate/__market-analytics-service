package models

// SubscriptionType определяет тип данных для подписки
type SubscriptionType string

const (
	SubTypeCandles      SubscriptionType = "candles"
	SubTypeOrderBook    SubscriptionType = "orderbook"
	SubTypeOrderBookAgg SubscriptionType = "orderbook_agg"
	SubTypeAlerts       SubscriptionType = "alerts"
)

// Subscription представляет параметры подписки на данные
type Subscription struct {
	Type        SubscriptionType `json:"type"`
	Exchange    string           `json:"exchange"`
	TradingPair string           `json:"trading_pair"`
	Interval    string           `json:"interval,omitempty"` // Для свечей
	Window      string           `json:"window,omitempty"`   // Окно агрегации
	Depth       int              `json:"depth,omitempty"`    // Глубина стакана
}

// SubscriptionRequest представляет запрос на подписку
type SubscriptionRequest struct {
	Action        string         `json:"action"` // subscribe/unsubscribe
	Subscriptions []Subscription `json:"subscriptions"`
}

// SubscriptionResponse представляет ответ на запрос подписки
type SubscriptionResponse struct {
	Success bool             `json:"success"`
	Error   string           `json:"error,omitempty"`
	Type    SubscriptionType `json:"type,omitempty"`
}
