package storage

import (
	"context"
	"fmt"
	"local/data-manager/internal/configuration"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Cache interface {
	Save(ctx context.Context, id string, value []byte) error
	Delete(ctx context.Context, id string) error
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
		DB:       config.DB,
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

func (r *RedisCache) Save(ctx context.Context, id string, value []byte) error {
	key := r.createKey(ctx, id)
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

func (r *RedisCache) createKey(ctx context.Context, id string) string {
	if id != "" {
		return fmt.Sprintf("%s:%s", r.namespace, id)
	}

	newID, err := r.rdbClient.Incr(ctx, "events:id:seq").Result()
	if err == nil {
		return fmt.Sprintf("%s:%d", r.namespace, newID)
	}

	uuid := uuid.New()
	return fmt.Sprintf("%s:%s", r.namespace, uuid.String())
}
