.PHONY: dev build run test clean docker-build docker-run

AIR := $(shell command -v air 2> /dev/null)

dev:
ifndef AIR
	$(error "air is not installed. Run: go install github.com/cosmtrek/air@latest")
endif
	air

# Build the application
build:
	go build -o bin/analytics-service ./cmd/server

# Run the application
run:
	go run ./cmd/server

# Run tests
test:
	go test -v ./...

# Clean build artifacts
clean:
	rm -rf bin/

# Build docker image
docker-build:
	docker-compose build

# Run with docker-compose
docker-run:
	docker-compose up

# Stop docker containers
docker-stop:
	docker-compose down

# Show logs
docker-logs:
	docker-compose logs -f

# Initialize development environment
init:
	cp .env.example .env
	go mod download
	go mod verify

# Run linter
lint:
	golangci-lint run

.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make dev         - Run the application with air"
	@echo "  make build       - Build the application"
	@echo "  make run        - Run the application locally"
	@echo "  make test       - Run tests"
	@echo "  make clean      - Clean build artifacts"
	@echo "  make docker-build - Build docker image"
	@echo "  make docker-run - Run with docker-compose"
	@echo "  make docker-stop - Stop docker containers"
	@echo "  make docker-logs - Show container logs"
	@echo "  make init       - Initialize development environment"
	@echo "  make lint       - Run linter"
