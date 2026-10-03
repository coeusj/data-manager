package injestion

import (
	"fmt"
	"time"
)

type EventKey struct {
	Id        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
}

func (e EventKey) ToDomain() *Key {
	return &Key{
		Id:        e.Id,
		Timestamp: e.Timestamp,
	}
}

type Event struct {
	Id         string    `json:"id"`
	ResourceId string    `json:"resourceId"`
	Type       string    `json:"type"`
	Start      time.Time `json:"start"`
	End        time.Time `json:"end"`
}

func (e Event) ToDomain() (*Model, error) {
	model := &Model{
		Id:         e.Id,
		ResourceId: e.ResourceId,
		Type:       e.Type,
		Start:      e.Start,
		End:        e.End,
	}

	if err := model.Validate(); err != nil {
		return nil, fmt.Errorf("failed to event to model %w", err)
	}

	return model, nil
}
