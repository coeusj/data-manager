package persistence

import (
	"context"
	"fmt"
	"local/data-manager/internal/configuration"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	rdbClient      *redis.Client
	logger         *slog.Logger
	namespace      string
	dataTTLSeconds time.Duration
}

func NewRedisCache(ctx context.Context, config configuration.RedisConfig, logger *slog.Logger) (*RedisCache, error) {
	rdbClient := redis.NewClient(&redis.Options{
		Addr:     config.Address,
		Password: "",
		DB:       config.DB,
	})

	if err := rdbClient.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	logger.Info("connected to redis")

	return &RedisCache{
		rdbClient:      rdbClient,
		logger:         logger,
		namespace:      config.NameSpace,
		dataTTLSeconds: time.Duration(config.DataTTLSeconds) * time.Second,
	}, nil
}

func (s *RedisCache) Save(ctx context.Context, id string, value []byte) error {
	key := s.createKey(ctx, id)
	err := s.rdbClient.Set(ctx, key, value, s.dataTTLSeconds).Err()
	if err != nil {
		return err
	}

	s.logger.Info("data saved successfully")
	return nil
}

func (s *RedisCache) Delete(ctx context.Context, id string) error {
	key := fmt.Sprintf("%s:%s", s.namespace, id)
	if err := s.rdbClient.Del(ctx, key).Err(); err != nil {
		return err
	}

	s.logger.Info("data deleted successfully")
	return nil
}

func (s *RedisCache) createKey(ctx context.Context, id string) string {
	if id != "" {
		return fmt.Sprintf("%s:%s", s.namespace, id)
	}

	newId, err := s.rdbClient.Incr(ctx, "events:id:seq").Result()
	if err == nil {
		return fmt.Sprintf("%s:%d", s.namespace, newId)
	}

	uuid := uuid.New()
	return fmt.Sprintf("%s:%s", s.namespace, uuid.String())
}
