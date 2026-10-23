package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"local/data-manager/internal/injestion"
	"local/data-manager/internal/websocket"
	"log/slog"
)

type CacheServiceWorker struct {
	cache             Cache
	hub               *websocket.Hub
	commitKafkaOffset func(partition int32, offset int64)
	logger            *slog.Logger
}

func NewCacheServiceWorker(cache Cache, hub *websocket.Hub, commitKafkaOffset func(partition int32, offset int64), logger *slog.Logger) *CacheServiceWorker {
	return &CacheServiceWorker{
		cache:             cache,
		hub:               hub,
		commitKafkaOffset: commitKafkaOffset,
		logger:            logger,
	}
}

func (w *CacheServiceWorker) Start(ctx context.Context, dataChannel <-chan *injestion.Payload) {
	w.logger.Info("cache service worker starting")
	for payload := range dataChannel {
		select {
		case <-ctx.Done():
			w.logger.Info("cache service worker execution cancelled")
			return
		default:
			w.logger.Info("processing payload", "id", &payload.Key.Id)

			if payload.IsDelete {
				if err := w.cache.Delete(ctx, payload.Key.Id); err != nil {
					w.logger.Error("could not delete data", "error", err)
				}
				// TODO: should also send a deletion message to hub
				continue
			}

			if err := w.save(ctx, payload.Key, payload.Value); err != nil {
				w.logger.Error("could not save payload", "error", err)
				continue
			}

			w.commitKafkaOffset(payload.Partition, payload.Offset)

			msg, err := json.Marshal(payload.Value)
			if err != nil {
				w.logger.Error("failed to marshal payload message", "error", err)
			}
			w.hub.Send(msg)
		}
	}
}

func (w *CacheServiceWorker) save(ctx context.Context, eventKey *injestion.Key, model *injestion.Message) error {
	payload, err := json.Marshal(model)
	if err != nil {
		return fmt.Errorf("failed to marshal model: %v", err)
	}

	if err := w.cache.Save(ctx, eventKey.Id, payload); err != nil {
		return err
	}

	return nil
}
