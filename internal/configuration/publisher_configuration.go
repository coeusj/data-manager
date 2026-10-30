package configuration

import (
	"encoding/json"
	"fmt"
	"os"
)

type GenLoopConfig struct {
	TickMs          float32 `json:"tickMs"`
	DurationSeconds float32 `json:"durationSeconds"`
}

type ProducerConfig struct {
	UseBatch         bool `json:"useBatch"`
	BatchSize        int  `json:"batchSize"`
	FlushFrequencyMs uint `json:"flushFrequencyMs"`
}

type PublisherConfig struct {
	Kafka    KafkaConfig    `json:"kafka"`
	GenLoop  GenLoopConfig  `json:"genLoop"`
	Producer ProducerConfig `json:"producer"`
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
			GroupID: "",
		},
		GenLoop: GenLoopConfig{
			TickMs:          1000,
			DurationSeconds: 1,
		},
		Producer: ProducerConfig{
			UseBatch:         false,
			BatchSize:        0,
			FlushFrequencyMs: 50,
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

	if c.GenLoop.DurationSeconds < 1 {
		return fmt.Errorf("duration should be a positive number")
	}

	if c.GenLoop.TickMs < 1 {
		return fmt.Errorf("tick should be a postive number")
	}

	if c.Producer.UseBatch && c.Producer.BatchSize < 1 {
		return fmt.Errorf("batch size should be a positive number")
	}

	return nil
}
