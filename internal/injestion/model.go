package injestion

import (
	"errors"
	"time"
)

type Payload struct {
	Key   Key
	Value Message
}

type Key struct {
	Id        string
	Timestamp time.Time
}

type Message struct {
	Id         string
	ResourceId string
	Type       string
	Start      time.Time
	End        time.Time
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
