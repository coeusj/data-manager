package persistence

import (
	"context"
	"fmt"
	"local/data-manager/internal/configuration"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type Storage struct {
	redisClient *redis.Client
	logger      *slog.Logger
}

func NewStorage(ctx context.Context, config configuration.RedisConfig, logger *slog.Logger) (*Storage, error) {
	rdbClient := redis.NewClient(&redis.Options{
		Addr:     config.Address,
		Password: "",
		DB:       0, // Default DB
	})

	if err := rdbClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	logger.Info("connected to redis")

	return &Storage{
		redisClient: rdbClient,
		logger:      logger,
	}, nil
}

func (s *Storage) Save(ctx context.Context, id string, value []byte) error {
	key := fmt.Sprintf("events:%s", id)

	if id == "" {
		newId, err := s.redisClient.Incr(ctx, "events:id:seq").Result()
		if err != nil {
			return fmt.Errorf("failed to auto-generate ID for event: %w", err)
		}

		key = fmt.Sprintf("events:%d", newId)
	}

	err := s.redisClient.Set(ctx, key, value, 5*time.Minute).Err()
	if err != nil {
		return fmt.Errorf("failed to save data into redis: %v", err)
	}

	s.logger.Info("data saved successfully")
	return nil
}
