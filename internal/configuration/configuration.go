package configuration

import (
	"encoding/json"
	"fmt"
	"os"
)

type KafkaConfig struct {
	Brokers string `json:"brokers"`
	Topic   string `json:"topic"`
	GroupId string `json:"groupId"`
}

type RedisConfig struct {
	Address string `json:"address"`
}

type Config struct {
	Kafka KafkaConfig `json:"kafka"`
	Redis RedisConfig `json:"redis"`
}

func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}

	defer file.Close()

	conf := Config{
		Kafka: KafkaConfig{
			Brokers: "",
			Topic:   "",
			GroupId: "default",
		},
		Redis: RedisConfig{
			Address: "",
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

func (c *Config) Validate() error {
	if c.Kafka.Brokers == "" {
		return fmt.Errorf("kafka brokers is required")
	}

	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka topic is missing")
	}

	if c.Redis.Address == "" {
		return fmt.Errorf("redis address is required")
	}

	return nil
}
