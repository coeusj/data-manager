package injestion

import (
	"context"
	"encoding/json"
	"fmt"
	"local/data-manager/internal/configuration"
	"local/data-manager/internal/persistence"
	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	kafkaConsumer *kafka.Consumer
	logger        *slog.Logger
	storage       *persistence.Storage
}

func NewConsumer(kafkaConfig configuration.KafkaConfig, storage *persistence.Storage, logger *slog.Logger) (*Consumer, error) {
	consumerConfig := kafka.ConfigMap{
		"bootstrap.servers":  kafkaConfig.Brokers,
		"group.id":           kafkaConfig.GroupId,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false, // Disable auto-commit for at-least-once semantics
	}

	consumer, err := kafka.NewConsumer(&consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	if err := consumer.Subscribe(kafkaConfig.Topic, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic: %w", err)
	}

	return &Consumer{
		kafkaConsumer: consumer,
		logger:        logger,
		storage:       storage,
	}, nil
}

func (c *Consumer) Start(ctx context.Context) error {
	defer func() {
		c.logger.Info("closing kafka consumer connection")
		if err := c.kafkaConsumer.Close(); err != nil {
			c.logger.Error("failed to close kafka consumer cleanly", "error", err)
		}
	}()

	c.logger.Info("kafka consumer loop started")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("context canceled, exiting consumer loop")
			return nil
		default:
			// Poll for events with a 100ms timeout to keep loop responsive to ctx.Done()
			event := c.kafkaConsumer.Poll(100)
			if event == nil {
				continue
			}

			if err := c.consume(ctx, event); err != nil {
				c.logger.Error("failed to consume event", "error", err)
				continue
			}
		}
	}
}

func (c *Consumer) consume(ctx context.Context, event kafka.Event) error {
	switch e := event.(type) {
	case *kafka.Message:
		if err := c.process(ctx, e); err != nil {
			c.logger.Error("failed to process kafka message",
				"topic", *e.TopicPartition.Topic,
				"partition", e.TopicPartition.Partition,
				"offset", e.TopicPartition.Offset,
				"error", err,
			)
			return err
		}

		if _, err := c.kafkaConsumer.CommitMessage(e); err != nil {
			c.logger.Error("failed to commit message offset", "error", err)
			return err
		}
	case kafka.Error:
		if e.IsFatal() {
			c.logger.Error("fatal kafka client error", "error", e)
			return e
		}
		c.logger.Warn("non-fatal kafka error encountered", "error", e)
		return e
	default:
		// Ignores internal events like partition assignments or rebalances
	}

	return nil
}

func (c *Consumer) process(ctx context.Context, msg *kafka.Message) error {
	c.logger.Info("processing message",
		"topic", *msg.TopicPartition.Topic,
		"partition", msg.TopicPartition.Partition,
		"offset", msg.TopicPartition.Offset,
		"key", string(msg.Key),
	)

	var event Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		c.logger.Error("failed to unmarshal kafka message payload",
			"offset", msg.TopicPartition.Offset,
			"partition", msg.TopicPartition.Partition,
			"error", err,
		)
		return err
	}

	var eventKey EventKey
	if err := json.Unmarshal(msg.Key, &eventKey); err != nil {
		c.logger.Warn("failed to unmarshal kafka message key")
		return err
	}

	model, err := event.ToDomain()
	if err != nil {
		c.logger.Error("failed to map event to domain model", "error", err)
		return err
	}

	if err := c.save(ctx, eventKey, model); err != nil {
		c.logger.Error("failed to save data into redis", "error", err)
		return err
	}

	return nil
}

func (c *Consumer) save(ctx context.Context, eventKey EventKey, model *Model) error {
	payload, err := json.Marshal(model)
	if err != nil {
		return fmt.Errorf("failed to marshal model: %v", err)
	}

	if err := c.storage.Save(ctx, eventKey.Id, payload); err != nil {
		return err
	}

	return nil
}
