package api

import (
	"fmt"
	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func GetEventsHandler(ctx *gin.Context, logger *slog.Logger) (map[string]string, error) {
	rdbClient := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "",
		DB:       0,
	})

	if err := rdbClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	results := make(map[string]string)
	iter := rdbClient.Scan(ctx, 0, "events:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		val, err := rdbClient.Get(ctx, key).Result()

		if err == redis.Nil {
			continue // Key expired during scan
		}

		if err != nil {
			return nil, err
		}
		results[key] = val
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return results, nil
}
