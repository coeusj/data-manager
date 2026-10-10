package injestion

import (
	"context"
	"fmt"
	v1 "local/data-manager/gen/go/event/v1"
	"local/data-manager/internal/configuration"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

type KafkaConsumer struct {
	kConsumer   *kafka.Consumer
	dataChannel chan<- *Payload
	logger      *slog.Logger
}

func NewKafkaConsumer(kafkaConfig configuration.KafkaConfig, dataChannel chan<- *Payload, logger *slog.Logger) (*KafkaConsumer, error) {
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

	return &KafkaConsumer{
		kConsumer:   consumer,
		dataChannel: dataChannel,
		logger:      logger,
	}, nil
}

func (c *KafkaConsumer) Start(ctx context.Context) error {
	defer func() {
		c.logger.Info("closing connection")
		if err := c.kConsumer.Close(); err != nil {
			c.logger.Error("failed to close kafka consumer cleanly", "error", err)
		}
	}()

	c.logger.Info("consume loop starting")

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

			if err := c.consume(event); err != nil {
				c.logger.Error("failed to consume event", "error", err)
				continue
			}
		}
	}
}

func (c *KafkaConsumer) consume(event kafka.Event) error {
	switch e := event.(type) {
	case *kafka.Message:
		if err := c.process(e); err != nil {
			c.logger.Error("failed to process message",
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

func (c *KafkaConsumer) process(msg *kafka.Message) error {
	c.logger.Info("processing message",
		"topic", *msg.TopicPartition.Topic,
		"partition", msg.TopicPartition.Partition,
		"offset", msg.TopicPartition.Offset,
		"key", string(msg.Key),
	)

	if msg.Value == nil {
		if err := c.remove(msg); err != nil {
			return err
		}
		return nil
	}

	if err := c.update(msg); err != nil {
		return err
	}

	return nil
}

func (c *KafkaConsumer) update(msg *kafka.Message) error {
	var eventKey v1.PBEventKey
	if err := proto.Unmarshal(msg.Key, &eventKey); err != nil {
		return fmt.Errorf("failed to unmarshal protobuff payload 'msg.Key'")
	}

	var eventMesage v1.PBEventMessage
	if err := proto.Unmarshal(msg.Value, &eventMesage); err != nil {
		return fmt.Errorf("failed to unmarshal protobuff payload 'msg.Value'")
	}

	c.dataChannel <- &Payload{
		Key: &Key{
			Id:        eventKey.EventId,
			Timestamp: time.Unix(eventKey.Timestamp, 0).UTC(),
		},
		Value: &Message{
			Id:         eventMesage.EventId,
			ResourceId: eventMesage.ResourceId,
			Type:       eventMesage.Type,
			Start:      time.Unix(eventMesage.Start, 0).UTC(),
			End:        time.Unix(eventMesage.End, 0).UTC().UTC(),
		},
		IsDelete: false,
		OnSuccess: func() {
			if _, err := c.kConsumer.CommitMessage(msg); err != nil {
				c.logger.Error("failed to commit message offset", "error", err)
			}
			c.logger.Info("message commited")
		},
	}

	c.logger.Info("message pushed in queue")
	return nil
}

func (c *KafkaConsumer) remove(msg *kafka.Message) error {
	var eventKey v1.PBEventKey
	if err := proto.Unmarshal(msg.Key, &eventKey); err != nil {
		return fmt.Errorf("failed to unmarshal protobuff payload 'msg.Key'")
	}

	c.dataChannel <- &Payload{
		Key: &Key{
			Id:        eventKey.EventId,
			Timestamp: time.Unix(eventKey.Timestamp, 0).UTC(),
		},
		Value:    &Message{},
		IsDelete: true,
		OnSuccess: func() {
			if _, err := c.kConsumer.CommitMessage(msg); err != nil {
				c.logger.Error("failed to commit message offset", "error", err)
			}
			c.logger.Info("message commited")
		},
	}

	return nil
}
