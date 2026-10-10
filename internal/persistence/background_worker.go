package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"local/data-manager/internal/injestion"
	"log/slog"
)

type BackgroundWorker struct {
	storage *RedisStorage
	logger  *slog.Logger
}

func NewBackgroundWorker(storage *RedisStorage, logger *slog.Logger) *BackgroundWorker {
	return &BackgroundWorker{
		storage: storage,
		logger:  logger,
	}
}

func (w *BackgroundWorker) Start(ctx context.Context, dataChannel <-chan *injestion.Payload) {
	w.logger.Info("background worker starting")

	for payload := range dataChannel {
		select {
		case <-ctx.Done():
			w.logger.Info("background worker execution cancelled")
			return
		default:
			w.logger.Info("processing payload", "id", &payload.Key.Id)

			if payload.IsDelete {
				if err := w.storage.Delete(ctx, payload.Key.Id); err != nil {
					w.logger.Error("could not delete data", "error", err)
					continue
				}
				continue
			}

			if err := w.save(ctx, payload.Key, payload.Value); err != nil {
				w.logger.Error("could not save payload", "error", err)
				continue
			}

			if payload.OnSuccess != nil {
				payload.OnSuccess()
			}
		}
	}
}

func (w *BackgroundWorker) save(ctx context.Context, eventKey *injestion.Key, model *injestion.Message) error {
	payload, err := json.Marshal(model)
	if err != nil {
		return fmt.Errorf("failed to marshal model: %v", err)
	}

	if err := w.storage.Save(ctx, eventKey.Id, payload); err != nil {
		return err
	}

	return nil
}
