package cache

import (
	"sync"
	"time"
)

type DataType string

const (
    TypeCandles      DataType = "candles"
    TypeOrderBook    DataType = "orderbook"
    TypeOrderBookAgg DataType = "orderbook_agg"
)

// CacheKey уникально идентифицирует набор данных
type CacheKey struct {
    Type        DataType
    Exchange    string
    TradingPair string
    Interval    string // Для свечей
}

// CacheEntry содержит закешированные данные и метаданные
type CacheEntry struct {
    Data      interface{}
    Timestamp time.Time
}

// Store представляет кэш данных
type Store struct {
    data     map[CacheKey]*CacheEntry
    watchers map[CacheKey][]chan interface{}
    ttl      time.Duration
    mu       sync.RWMutex
}

func NewStore(ttl time.Duration) *Store {
    s := &Store{
        data:     make(map[CacheKey]*CacheEntry),
        watchers: make(map[CacheKey][]chan interface{}),
        ttl:      ttl,
    }

    go s.cleanup()
    return s
}

// Set сохраняет данные в кэш и уведомляет подписчиков
func (s *Store) Set(key CacheKey, data interface{}) {
    s.mu.Lock()
    s.data[key] = &CacheEntry{
        Data:      data,
        Timestamp: time.Now(),
    }
    watchers := s.watchers[key]
    s.mu.Unlock()

    // Уведомляем всех подписчиков
    for _, ch := range watchers {
        select {
        case ch <- data:
        default:
            // Если канал заполнен, пропускаем
        }
    }
}

// Get возвращает данные из кэша
func (s *Store) Get(key CacheKey) (interface{}, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    entry, exists := s.data[key]
    if !exists || time.Since(entry.Timestamp) > s.ttl {
        return nil, false
    }
    return entry.Data, true
}

// Watch подписывается на обновления данных
func (s *Store) Watch(key CacheKey) (<-chan interface{}, func()) {
    ch := make(chan interface{}, 100)

    s.mu.Lock()
    s.watchers[key] = append(s.watchers[key], ch)
    s.mu.Unlock()

    // Возвращаем функцию отписки
    return ch, func() {
        s.mu.Lock()
        defer s.mu.Unlock()

        watchers := s.watchers[key]
        for i, watcher := range watchers {
            if watcher == ch {
                s.watchers[key] = append(watchers[:i], watchers[i+1:]...)
                break
            }
        }
        close(ch)
    }
}

// cleanup периодически удаляет устаревшие данные
func (s *Store) cleanup() {
    ticker := time.NewTicker(s.ttl / 2)
    defer ticker.Stop()

    for range ticker.C {
        s.mu.Lock()
        now := time.Now()
        for key, entry := range s.data {
            if now.Sub(entry.Timestamp) > s.ttl {
                delete(s.data, key)
            }
        }
        s.mu.Unlock()
    }
}
