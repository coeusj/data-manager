package configuration

import (
	"encoding/json"
	"fmt"
	"os"
)

type ReaderConfig struct {
	Kafka KafkaConfig `json:"kafka"`
	Redis RedisConfig `json:"redis"`
}

func LoadReaderConfig(path string) (*ReaderConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}

	defer file.Close()

	conf := ReaderConfig{
		Kafka: KafkaConfig{
			Brokers: "",
			Topic:   "",
			GroupID: "default",
		},
		Redis: RedisConfig{
			Address:        "",
			Namespace:      "",
			DataTTLSeconds: 0,
			DB:             0,
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
	if c.Kafka.Brokers == "" {
		return fmt.Errorf("kafka brokers is required")
	}

	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka topic is missing")
	}

	if c.Redis.Address == "" {
		return fmt.Errorf("redis address is required")
	}

	if c.Redis.Namespace == "" {
		return fmt.Errorf("redis namespace is missing")
	}

	return nil
}
