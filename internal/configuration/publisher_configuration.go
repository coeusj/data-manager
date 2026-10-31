package configuration

import (
	"encoding/json"
	"fmt"
	"os"
)

type ProducerConfig struct {
	Topic            string  `json:"topic"`
	TickMs           float32 `json:"tickMs"`
	DurationSeconds  float32 `json:"durationSeconds"`
	UseBatch         bool    `json:"useBatch"`
	BatchSize        int     `json:"batchSize"`
	FlushFrequencyMs uint    `json:"flushFrequencyMs"`
}

type PublisherConfig struct {
	Producer ProducerConfig `json:"producer"`
}

func LoadPublisherConfig(path string) (*PublisherConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}

	defer file.Close()

	conf := PublisherConfig{
		Producer: ProducerConfig{
			TickMs:           1000,
			DurationSeconds:  1,
			UseBatch:         false,
			BatchSize:        1,
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
	if c.Producer.Topic == "" {
		return fmt.Errorf("topic is required")
	}

	return nil
}
