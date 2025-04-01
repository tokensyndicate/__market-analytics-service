package ws

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"

	"market-analytics-service/pkg/models"
)

type Connection struct {
	conn          *websocket.Conn
	subscriptions map[models.SubscriptionType][]models.Subscription
	clientID      string
	manager       *SubscriptionManager
	mu            sync.RWMutex
	cancelFuncs   []context.CancelFunc
	closed        atomic.Bool
}

func NewConnection(conn *websocket.Conn, clientID string, manager *SubscriptionManager) *Connection {
	return &Connection{
		conn:          conn,
		subscriptions: make(map[models.SubscriptionType][]models.Subscription),
		clientID:      clientID,
		manager:       manager,
		cancelFuncs:   make([]context.CancelFunc, 0),
	}
}

func (c *Connection) Start() {
	// Читаем сообщения от клиента
	go func() {
		defer func() {
			// Перехватываем возможную панику при чтении из закрытого соединения
			if r := recover(); r != nil {
				log.Error().
					Interface("recover", r).
					Str("clientID", c.clientID).
					Msg("Recovered from panic in WebSocket read loop")
			}

			// Гарантируем закрытие соединения при выходе
			c.Close()
		}()

		for {
			// Проверяем, закрыто ли соединение
			if c.IsClosed() {
				return
			}

			var req models.SubscriptionRequest
			err := c.conn.ReadJSON(&req)
			if err != nil {
				log.Info().
					Str("clientID", c.clientID).
					Err(err).
					Msg("Connection closed or read error")
				return
			}

			log.Info().
				Str("clientID", c.clientID).
				Interface("request", req).
				Msg("Received subscription request")

			c.handleSubscriptionRequest(req)
		}
	}()
}

func (c *Connection) SendMessage(msg interface{}) error {
	if c.IsClosed() {
		return fmt.Errorf("connection is closed")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("connection is closed")
	}

	// Добавляем защиту от паники
	defer func() {
		if r := recover(); r != nil {
			log.Error().
				Interface("recover", r).
				Str("clientID", c.clientID).
				Msg("Recovered from panic in SendMessage")
		}
	}()

	err := c.conn.WriteJSON(msg)
	if err != nil {
		log.Error().
			Err(err).
			Str("clientID", c.clientID).
			Interface("message", msg).
			Msg("Failed to send message")
		return err
	}

	log.Debug().
		Str("clientID", c.clientID).
		Interface("messageType", msg.(models.WSMessage).Type).
		Msg("Message sent successfully")

	return nil
}

func (c *Connection) AddCancelFunc(cancel context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelFuncs = append(c.cancelFuncs, cancel)
}

func (c *Connection) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Swap(true) {
		return // Уже закрыто
	}

	// Отменяем все подписки
	for _, cancel := range c.cancelFuncs {
		cancel()
	}
	c.cancelFuncs = nil

	// Очищаем все подписки
	c.subscriptions = make(map[models.SubscriptionType][]models.Subscription)

	// Безопасно закрываем WebSocket соединение
	if c.conn != nil {
		// Игнорируем ошибки при отправке сообщения о закрытии
		_ = c.conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		_ = c.conn.Close() // Игнорируем возможные ошибки
		c.conn = nil
	}

	log.Info().
		Str("clientID", c.clientID).
		Msg("Connection fully closed")
}

func (c *Connection) IsClosed() bool {
	return c.closed.Load()
}

func (c *Connection) GetClientID() string {
	return c.clientID
}

func (c *Connection) handleSubscriptionRequest(req models.SubscriptionRequest) {
	log.Debug().
		Str("clientID", c.clientID).
		Interface("request", req).
		Msg("Processing subscription request")

	if err := c.manager.ProcessSubscriptionRequest(c, req); err != nil {
		log.Error().
			Err(err).
			Str("clientID", c.clientID).
			Interface("request", req).
			Msg("Failed to process subscription request")

		// Отправляем только сообщение об ошибке
		errMsg := models.WSMessage{
			Type:  "error",
			Error: err.Error(),
		}
		c.SendMessage(errMsg)
		return
	}

	// Сохраняем подписки локально
	c.mu.Lock()
	defer c.mu.Unlock()

	switch req.Action {
	case "subscribe":
		for _, sub := range req.Subscriptions {
			c.subscriptions[sub.Type] = append(c.subscriptions[sub.Type], sub)
		}
	case "unsubscribe":
		for _, sub := range req.Subscriptions {
			delete(c.subscriptions, sub.Type)
		}
	}
}
