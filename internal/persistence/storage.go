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
	rdbClient      *redis.Client
	logger         *slog.Logger
	namespace      string
	dataTTLSeconds time.Duration
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
		rdbClient:      rdbClient,
		logger:         logger,
		namespace:      config.NameSpace,
		dataTTLSeconds: time.Duration(config.DataTTLSeconds) * time.Second,
	}, nil
}

func (s *Storage) Save(ctx context.Context, id string, value []byte) error {
	key := fmt.Sprintf("%s:%s", s.namespace, id)

	if id == "" {
		newId, err := s.rdbClient.Incr(ctx, "events:id:seq").Result()
		if err != nil {
			return fmt.Errorf("failed to auto-generate ID for event: %w", err)
		}

		key = fmt.Sprintf("%s:%d", s.namespace, newId)
	}

	err := s.rdbClient.Set(ctx, key, value, s.dataTTLSeconds).Err()
	if err != nil {
		return fmt.Errorf("failed to save data into redis: %v", err)
	}

	s.logger.Info("data saved successfully")
	return nil
}

func (s *Storage) Delete(ctx context.Context, id string) error {
	key := fmt.Sprintf("%s:%s", s.namespace, id)
	if err := s.rdbClient.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to delete data: %w", err)
	}

	s.logger.Info("data deleted successfully")
	return nil
}
