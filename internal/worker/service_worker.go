package worker

import (
	"context"
	"encoding/json"
	"local/data-manager/internal/injestion"
	"local/data-manager/internal/realtime"
	"local/data-manager/internal/storage"
	"log/slog"
)

type Committer func(partition int32, offset int64)

type ServiceWorker[K any, V any] struct {
	cache       storage.Cache
	broadcaster realtime.Broadcaster
	commit      Committer
	logger      *slog.Logger
}

func NewServiceWorker[K any, V any](
	cache storage.Cache,
	broadcaster realtime.Broadcaster,
	commit Committer,
	logger *slog.Logger) *ServiceWorker[K, V] {
	return &ServiceWorker[K, V]{
		cache:       cache,
		broadcaster: broadcaster,
		commit:      commit,
		logger:      logger,
	}
}

func (s *ServiceWorker[K, V]) Start(ctx context.Context, dataChannel <-chan *injestion.Payload[K, V]) {
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
				if err := s.cache.Delete(ctx, payload.ID); err != nil {
					s.logger.Error("could not delete data", "error", err)
				}
				continue // TODO: should also send a deletion message to hub
			}

			if err := s.save(ctx, payload.ID, payload.Value); err != nil {
				s.logger.Error("could not save payload", "error", err)
				continue // TODO: should retry
			}

			s.commit(payload.Partition, payload.Offset)
			s.broadcast(ctx, payload)
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

func (s *ServiceWorker[K, V]) broadcast(ctx context.Context, payload *injestion.Payload[K, V]) {
	msg, err := json.Marshal(payload.Value)
	if err != nil {
		s.logger.Error("failed to marshal payload message", "error", err)
	}

	s.broadcaster.Broadcast(ctx, msg)
}
