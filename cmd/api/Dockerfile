FROM golang:1.27.3-alpine AS builder

RUN apk add --no-cache ca-certificates git

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o api ./cmd/api

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata
RUN adduser -D -g '' appuser

WORKDIR /app

COPY --from=builder /app/api .

ENV GIN_MODE=release

USER appuser

EXPOSE 8080

ENTRYPOINT ["./api"]