package configuration

import (
	"encoding/json"
	"fmt"
	"os"
)

type SenderConfig struct {
	TickMs          float32 `json:"tickMs"`
	DurationSeconds float32 `json:"durationSeconds"`
}

type PublisherConfig struct {
	Kafka  KafkaConfig  `json:"kafka"`
	Sender SenderConfig `json:"sender"`
}

func LoadPublisherConfig(path string) (*PublisherConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}

	defer file.Close()

	conf := PublisherConfig{
		Kafka: KafkaConfig{
			Brokers: "",
			Topic:   "",
			GroupId: "",
		},
		Sender: SenderConfig{
			TickMs:          1000,
			DurationSeconds: 1,
		},
	}

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&conf); err != nil {
		return nil, fmt.Errorf("failed to decode config JSON: %w", err)
	}

	if err := conf.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &conf, nil
}

func (c *PublisherConfig) Validate() error {
	if c.Kafka.Brokers == "" {
		return fmt.Errorf("kafka brokers is required")
	}

	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka topic is missing")
	}

	if c.Sender.DurationSeconds < 1 {
		return fmt.Errorf("duration should be a positive number")
	}

	if c.Sender.TickMs < 1 {
		return fmt.Errorf("tick should be a postive number")
	}

	return nil
}
