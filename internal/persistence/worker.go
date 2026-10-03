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

func (o *Worker) Start(ctx context.Context, dataChannel chan *injestion.Payload) {
	for payload := range dataChannel {
		select {
		case <-ctx.Done():
			o.logger.Info("buffer-job execution cancelled")
			return
		default:
			o.logger.Info("buffer-job processing event")
			if err := o.save(ctx, &payload.Key, &payload.Value); err != nil {
				o.logger.Error("could save payload", "error", err)
				continue
			}
		}
	}
}

func (o *Worker) save(ctx context.Context, eventKey *injestion.Key, model *injestion.Message) error {
	payload, err := json.Marshal(model)
	if err != nil {
		return fmt.Errorf("failed to marshal model: %v", err)
	}

	if err := o.storage.Save(ctx, eventKey.Id, payload); err != nil {
		return err
	}

	return nil
}
