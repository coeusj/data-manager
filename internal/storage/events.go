package storage

import (
	"context"
	"local/data-manager/internal/configuration"
	"log"
	"log/slog"
	"os"
	"strconv"
)

func NewEventsCache(ctx context.Context, conf *configuration.ReaderConfig, logger *slog.Logger) *RedisCache {
	url := os.Getenv("REDIS_URL")
	namespace := os.Getenv("REDIS_EVENTS_NAMESPACE")

	eventsDbIdxStr := os.Getenv("REDIS_EVENTS_DB_IDX")
	eventsDbIdx, err := strconv.Atoi(eventsDbIdxStr)
	if err != nil {
		log.Fatalf("invalid DB index: %v", err)
	}

	cache, err := NewRedisCache(ctx, url, eventsDbIdx, namespace, conf.Consumer.DataTTLSeconds, logger)
	if err != nil {
		log.Fatalf("could not create redis instance: %v", err)
	}

	return cache
}
