package configuration

import (
	"encoding/json"
	"fmt"
	"os"
)

type ConsumerConfig struct {
	Topic          string `json:"topic"`
	GroupID        string `json:"groupId"`
	DataTTLSeconds uint   `json:"dataTTLSeconds"`
	BufferSize     uint   `json:"bufferSize"`
}

type ReaderConfig struct {
	Consumer ConsumerConfig `json:"consumer"`
}

func LoadReaderConfig(path string) (*ReaderConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}

	defer file.Close()

	conf := ReaderConfig{
		Consumer: ConsumerConfig{
			DataTTLSeconds: 0, // infinite
			BufferSize:     100,
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

func (c *ReaderConfig) Validate() error {
	if c.Consumer.Topic == "" {
		return fmt.Errorf("topic is required")
	}

	if c.Consumer.GroupID == "" {
		return fmt.Errorf("group ID is required")
	}

	return nil
}
