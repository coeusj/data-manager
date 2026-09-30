FROM golang:1.25.5-alpine AS builder

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
    -o /app/bin/worker ./cmd/worker

FROM alpine:3.19

# Install runtime librdkafka dependency, TLS certificates, and timezones
RUN apk add --no-cache \
    librdkafka \
    ca-certificates \
    tzdata

WORKDIR /app

# Copy binary and config WITH appuser ownership
COPY --chown=10001:10001 --from=builder /app/bin/worker .
COPY --chown=10001:10001 config.json /app/config.json

ENV DM_CONFIG_PATH=/app/config.json

# Run as non-root user
RUN adduser -D -u 10001 appuser
USER appuser

ENTRYPOINT ["./worker"]