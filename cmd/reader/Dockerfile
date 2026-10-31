FROM golang:1.27.3-alpine AS builder

# Install build dependencies required for CGO and librdkafka on musl
RUN apk add --no-cache \
    gcc \
    musl-dev \
    libc-dev \
    pkgconf \
    librdkafka-dev \
    git \
    make

WORKDIR /app

# Cache Go modules layer
COPY go.mod go.sum ./
RUN go mod download

# Copy application source
COPY . .

# Build with CGO enabled and the musl tag
ENV CGO_ENABLED=1
RUN go build \
    -tags musl \
    -ldflags="-s -w" \
    -o /app/bin/reader ./cmd/reader

FROM alpine:3.20

# Install runtime librdkafka dependency, TLS certificates, and timezones
RUN apk add --no-cache \
    librdkafka \
    ca-certificates \
    tzdata

WORKDIR /app

# Copy binary and config WITH appuser ownership
COPY --chown=10001:10001 --from=builder /app/bin/reader .
COPY --chown=10001:10001 config.reader.json /app/config.json

ENV APP_ENV=release
ENV CONF_PATH=/app/config.json
ENV KAFKA_BROKERS=127.0.0.1:9092
ENV REDIS_URL=127.0.0.1:6379
ENV REDIS_EVENTS_DB_IDX=0
ENV REDIS_EVENTS_NAMESPACE=events
ENV WS_URL=127.0.0.1:8989

# Run as non-root user
RUN adduser -D -u 10001 appuser
USER appuser

EXPOSE 8989

ENTRYPOINT ["./reader"]