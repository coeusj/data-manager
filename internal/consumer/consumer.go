package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"local/data-manager/internal/configuration"
)

type Consumer struct {
	reader *kafka.Reader
	logger *slog.Logger
}

func New(kafkaConfig configuration.KafkaConfig, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     strings.Split(kafkaConfig.Broker, ","),
		Topic:       kafkaConfig.Topic,
		GroupID:     kafkaConfig.GroupId,
		MinBytes:    10e3, // 10KB minimum fetch
		MaxBytes:    10e6, // 10MB maximum fetch
		MaxWait:     1 * time.Second,
		StartOffset: kafka.FirstOffset, // Read from offset 0 / earliest message available
	})

	return &Consumer{
		reader: reader,
		logger: logger,
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	defer func() {
		if err := c.reader.Close(); err != nil {
			c.logger.Error("failed to close kafka reader", "error", err)
		}
	}()

	c.logger.Info("start listening for kafka messages", "topic", c.reader.Config().Topic)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				c.logger.Info("kafka consumer context cancelled, exiting loop")
				return nil
			}

			c.logger.Error("error fetching message from kafka", "error", err)
			time.Sleep(1 * time.Second) // Prevent tight loop during broker rebalance/outage
			continue
		}

		if err := c.processMessage(ctx, msg); err != nil {
			c.logger.Error("failed to process message",
				"offset", msg.Offset,
				"partition", msg.Partition,
				"error", err,
			)
			// Handle dead-letter queue (DLQ) or retry logic here
			continue
		}

		// Commit offset AFTER processing succeeds (At-Least-Once Semantics)
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("failed to commit offset", "error", err)
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) error {
	c.logger.Info("processing message",
		"key", string(msg.Key),
		"offset", msg.Offset,
		"partition", msg.Partition,
	)

	var event Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		c.logger.Error("failed to unmarshal kafka message payload",
			"offset", msg.Offset,
			"partition", msg.Partition,
			"error", err,
		)
		return fmt.Errorf("unmarshal error: %w", err)
	}

	model, err := event.ToDomain()
	if err != nil {
		c.logger.Error("failed to map event to domain model", "error", err)
		return err
	}

	fmt.Printf("message process successfully: %+v\n", model)
	return nil
}
