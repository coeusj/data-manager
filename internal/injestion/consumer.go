package injestion

import (
	"context"
	"encoding/json"
	"fmt"
	"local/data-manager/internal/configuration"
	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	kConsumer *kafka.Consumer
	logger    *slog.Logger
}

func NewConsumer(kafkaConfig configuration.KafkaConfig, logger *slog.Logger) (*Consumer, error) {
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
		kConsumer: consumer,
		logger:    logger,
	}, nil
}

func (c *Consumer) Start(ctx context.Context, dataChannel chan<- *Payload) error {
	defer func() {
		c.logger.Info("closing kafka consumer connection")
		if err := c.kConsumer.Close(); err != nil {
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
			event := c.kConsumer.Poll(100)
			if event == nil {
				continue
			}

			if err := c.consume(event, dataChannel); err != nil {
				c.logger.Error("failed to consume event", "error", err)
				continue
			}
		}
	}
}

func (c *Consumer) consume(event kafka.Event, dataChannel chan<- *Payload) error {
	// TODO: CHECK HOW TO GET KAFKA TOMBSTONES AND MESSAGE "DELETION"
	switch e := event.(type) {
	case *kafka.Message:
		if err := c.process(dataChannel, e); err != nil {
			c.logger.Error("failed to process kafka message",
				"topic", *e.TopicPartition.Topic,
				"partition", e.TopicPartition.Partition,
				"offset", e.TopicPartition.Offset,
				"error", err,
			)
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

func (c *Consumer) process(dataChannel chan<- *Payload, msg *kafka.Message) error {
	c.logger.Info("processing message",
		"topic", *msg.TopicPartition.Topic,
		"partition", msg.TopicPartition.Partition,
		"offset", msg.TopicPartition.Offset,
		"key", string(msg.Key),
	)

	var eventKey EventKey
	if err := json.Unmarshal(msg.Key, &eventKey); err != nil {
		c.logger.Warn("failed to unmarshal kafka message key")
		return err
	}

	var eventMessage EventMessage
	if err := json.Unmarshal(msg.Value, &eventMessage); err != nil {
		c.logger.Error("failed to unmarshal kafka message payload",
			"offset", msg.TopicPartition.Offset,
			"partition", msg.TopicPartition.Partition,
			"error", err,
		)
		return err
	}

	key := eventKey.ToDomain()
	message, err := eventMessage.ToDomain()
	if err != nil {
		c.logger.Error("failed to map event to domain model", "error", err)
		return err
	}

	dataChannel <- &Payload{
		Key:   *key,
		Value: *message,
		OnSuccess: func() {
			if _, err := c.kConsumer.CommitMessage(msg); err != nil {
				c.logger.Error("failed to commit message offset", "error", err)
			}
			c.logger.Info("message commited")
		},
	}

	return nil
}
