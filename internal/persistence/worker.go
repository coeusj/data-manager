package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"local/data-manager/internal/injestion"
	"log/slog"
)

type Worker struct {
	storage *Storage
	logger  *slog.Logger
}

func NewWorker(storage *Storage, logger *slog.Logger) *Worker {
	return &Worker{
		storage: storage,
		logger:  logger,
	}
}

func (w *Worker) Start(ctx context.Context, dataChannel <-chan *injestion.Payload) {
	w.logger.Info("starting worker")
	for payload := range dataChannel {
		select {
		case <-ctx.Done():
			w.logger.Info("worker execution cancelled")
			return
		default:
			w.logger.Info("worker processing payload", "id", &payload.Key.Id)
			if err := w.save(ctx, &payload.Key, &payload.Value); err != nil {
				w.logger.Error("could save payload", "error", err)
				continue
			}

			if payload.OnSuccess != nil {
				payload.OnSuccess()
			}
		}
	}
}

func (w *Worker) save(ctx context.Context, eventKey *injestion.Key, model *injestion.Message) error {
	payload, err := json.Marshal(model)
	if err != nil {
		return fmt.Errorf("failed to marshal model: %v", err)
	}

	if err := w.storage.Save(ctx, eventKey.Id, payload); err != nil {
		return err
	}

	return nil
}
