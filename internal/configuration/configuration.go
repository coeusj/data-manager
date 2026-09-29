package configuration

import (
	"encoding/json"
	"fmt"
	"os"
)

type KafkaConfig struct {
	Broker  string `json:"broker"`
	Topic   string `json:"topic"`
	GroupId string `json:"groupId"`
}

type Config struct {
	Kafka KafkaConfig `json:"kafka"`
}

func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}

	defer file.Close()

	conf := Config{
		Kafka: KafkaConfig{
			Broker:  "",
			Topic:   "",
			GroupId: "default",
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
	if c.Kafka.Broker == "" {
		return fmt.Errorf("kafka broker is required")
	}

	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka topic is missing")
	}

	return nil
}
