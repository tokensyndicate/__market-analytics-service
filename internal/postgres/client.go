package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

type Client struct {
	pool *pgxpool.Pool
}

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

// NewClient creates a new PostgreSQL client
func NewClient(cfg Config) (*Client, error) {
	connStr := fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.DBName,
	)

	ctx := context.Background()
	pool, err := pgxpool.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	return &Client{pool: pool}, nil
}

// GetUserClientIDs returns all client IDs associated with a user
func (c *Client) GetUserClientIDs(ctx context.Context, userID string) ([]string, error) {
	// query := `
	//     SELECT client_id
	//     FROM user_clients
	//     WHERE user_id = $1
	// `

	// rows, err := c.pool.Query(ctx, query, userID)
	// if err != nil {
	//     return nil, fmt.Errorf("failed to query client ids: %w", err)
	// }
	// defer rows.Close()

	// var clientIDs []string
	// for rows.Next() {
	//     var clientID string
	//     if err := rows.Scan(&clientID); err != nil {
	//         return nil, fmt.Errorf("failed to scan client id: %w", err)
	//     }
	//     clientIDs = append(clientIDs, clientID)
	// }

	// return clientIDs, nil

	// Temporary mock data
	return []string{}, nil
}

func (c *Client) Close() {
	if c.pool != nil {
		c.pool.Close()
	}
}
