package persistence

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"local/data-manager/internal/consumer"
	"local/data-manager/internal/websocket"
	"log/slog"
)

type ServiceWorker[K any, V any] struct {
	cache             Cache
	hub               *websocket.Hub
	commitKafkaOffset func(partition int32, offset int64)
	logger            *slog.Logger
}

func NewServiceWorker[K any, V any](
	cache Cache,
	hub *websocket.Hub,
	commitKafkaOffset func(partition int32, offset int64),
	logger *slog.Logger) *ServiceWorker[K, V] {
	return &ServiceWorker[K, V]{
		cache:             cache,
		hub:               hub,
		commitKafkaOffset: commitKafkaOffset,
		logger:            logger,
	}
}

func (s *ServiceWorker[K, V]) Start(ctx context.Context, dataChannel <-chan *consumer.Payload[K, V]) {
	defer s.cache.Close()

	s.logger.Info("cache service worker starting")

	for payload := range dataChannel {
		select {
		case <-ctx.Done():
			s.logger.Info("cache service worker execution cancelled")
			return
		default:
			s.logger.Info("processing payload")

			if payload.IsDelete {
				if err := s.cache.Delete(ctx, s.hashToBase64(payload.HashKey)); err != nil {
					s.logger.Error("could not delete data", "error", err)
				}
				continue // TODO: should also send a deletion message to hub
			}

			if err := s.save(ctx, s.hashToBase64(payload.HashKey), payload.Value); err != nil {
				s.logger.Error("could not save payload", "error", err)
				continue // TODO: should retry
			}

			s.commitKafkaOffset(payload.Partition, payload.Offset)
			s.sendToHub(payload)
		}
	}
}

func (s *ServiceWorker[K, V]) save(ctx context.Context, key string, payloadVal *V) error {
	val, err := json.Marshal(payloadVal)
	if err != nil {
		return err
	}

	if err := s.cache.Save(ctx, key, val); err != nil {
		return err
	}

	return nil
}

func (s *ServiceWorker[K, V]) sendToHub(payload *consumer.Payload[K, V]) {
	msg, err := json.Marshal(payload.Value)
	if err != nil {
		s.logger.Error("failed to marshal payload message", "error", err)
	}

	s.hub.Send(msg)
}

func (s *ServiceWorker[K, V]) hashToBase64(hashedKey [32]byte) string {
	base64Str := base64.StdEncoding.EncodeToString(hashedKey[:])
	return base64Str
}
