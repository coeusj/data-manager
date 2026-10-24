package consumer

import (
	"errors"
	"time"
)

type Payload struct {
	Key       *Key
	Value     *Message
	IsDelete  bool
	Partition int32
	Offset    int64
}

type Key struct {
	Id        string
	Timestamp time.Time
}

type Message struct {
	Id         string    `json:"id"`
	ResourceId string    `json:"resourceId"`
	Type       string    `json:"type"`
	Start      time.Time `json:"start"`
	End        time.Time `json:"end"`
}

func (c *Message) Validate() error {
	if c.Id == "" {
		return errors.New("model without ID")
	}

	if c.ResourceId == "" {
		return errors.New("model without Resource ID")
	}

	return nil
}
