package configuration

import (
	"encoding/json"
	"fmt"
	"os"
)

type WorkerConfig struct {
	Kafka KafkaConfig `json:"kafka"`
	Redis RedisConfig `json:"redis"`
}

func LoadWorkerConfig(path string) (*WorkerConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}

	defer file.Close()

	conf := WorkerConfig{
		Kafka: KafkaConfig{
			Brokers: "",
			Topic:   "",
			GroupId: "default",
		},
		Redis: RedisConfig{
			Address:        "",
			NameSpace:      "",
			DataTTLSeconds: 0,
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

func (c *WorkerConfig) Validate() error {
	if c.Kafka.Brokers == "" {
		return fmt.Errorf("kafka brokers is required")
	}

	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka topic is missing")
	}

	if c.Redis.Address == "" {
		return fmt.Errorf("redis address is required")
	}

	if c.Redis.NameSpace == "" {
		return fmt.Errorf("redis namespace is missing")
	}

	return nil
}
