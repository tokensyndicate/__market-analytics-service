# Build stage
FROM golang:1.22.2-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git make gcc musl-dev

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/analytics-service ./cmd/server

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/analytics-service .
COPY config/config.yaml ./config/

# Create non-root user
RUN adduser -D -H -h /app appuser && \
    chown -R appuser:appuser /app

USER appuser

EXPOSE 8080

CMD ["./analytics-service"]
