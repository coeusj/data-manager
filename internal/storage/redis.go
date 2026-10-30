package storage

import (
	"context"
	"fmt"
	"local/data-manager/internal/configuration"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache interface {
	Save(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	Close()
}

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
		DB:       config.DBIndex,
	})

	if err := rdbClient.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	logger.Info("connected to redis")

	return &RedisCache{
		rdbClient:      rdbClient,
		logger:         logger,
		namespace:      config.Namespace,
		dataTTLSeconds: time.Duration(config.DataTTLSeconds) * time.Second,
	}, nil
}

func (r *RedisCache) Save(ctx context.Context, key string, value []byte) error {
	err := r.rdbClient.Set(ctx, key, value, r.dataTTLSeconds).Err()
	if err != nil {
		return err
	}

	r.logger.Info("data saved successfully")
	return nil
}

func (r *RedisCache) Delete(ctx context.Context, id string) error {
	key := fmt.Sprintf("%s:%s", r.namespace, id)
	if err := r.rdbClient.Del(ctx, key).Err(); err != nil {
		return err
	}

	r.logger.Info("data deleted successfully")
	return nil
}

func (r *RedisCache) Close() {
	if err := r.rdbClient.Close(); err != nil {
		r.logger.Error("failed to close redis client", "error", err)
	}
}
