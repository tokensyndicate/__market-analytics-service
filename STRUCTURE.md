market-analytics-service/
├── Makefile
├── README.md
├── cmd/
│ └── server/
│ └── main.go
├── config/
│ └── config.yaml
├── internal/
│ ├── config/
│ │ └── config.go
│ ├── influx/
│ │ └── client.go
│ ├── server/
│ │ ├── server.go // общая настройка серверов
│ │ ├── websocket.go // WS хендлеры
│ │ └── http.go // HTTP хендлеры
│ └── service/
│ └── analytics.go // бизнес-логика
└── pkg/
└── types/
└── data.go // общие типы данных
