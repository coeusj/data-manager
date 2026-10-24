package consumer

import (
	"context"
	"fmt"
	v1 "local/data-manager/gen/go/event/v1"
	"local/data-manager/internal/configuration"
	"local/data-manager/internal/websocket"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

type KafkaConsumer struct {
	kConsumer     *kafka.Consumer
	topic         *string
	pollTimeoutMs int
	dataChannel   chan<- *Payload
	hub           *websocket.Hub
	logger        *slog.Logger
}

func NewKafkaConsumer(
	kafkaConfig configuration.KafkaConfig,
	dataChannel chan<- *Payload,
	hub *websocket.Hub,
	logger *slog.Logger) (*KafkaConsumer, error) {
	consumerConfig := kafka.ConfigMap{
		"bootstrap.servers":  kafkaConfig.Brokers,
		"group.id":           kafkaConfig.GroupId,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	}

	consumer, err := kafka.NewConsumer(&consumerConfig)
	if err != nil {
		return nil, err
	}

	if err := consumer.Subscribe(kafkaConfig.Topic, nil); err != nil {
		return nil, err
	}

	return &KafkaConsumer{
		kConsumer:     consumer,
		topic:         &kafkaConfig.Topic,
		pollTimeoutMs: 100, // Poll for events with a 100ms timeout to keep loop responsive to ctx.Done()
		dataChannel:   dataChannel,
		hub:           hub,
		logger:        logger,
	}, nil
}

func (c *KafkaConsumer) Run(ctx context.Context) {
	defer c.close()

	c.logger.Info("consume loop starting")

	for {
		select {
		case <-ctx.Done():
			c.logger.Warn("context canceled, exiting consumer loop")
			return
		default:
			event := c.kConsumer.Poll(c.pollTimeoutMs)
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

func (c *KafkaConsumer) Commit(partition int32, offset int64) {
	offsetsToCommit := []kafka.TopicPartition{
		{
			Topic:     c.topic,
			Partition: partition,
			Offset:    kafka.Offset(offset + 1),
		},
	}

	_, err := c.kConsumer.CommitOffsets(offsetsToCommit)
	if err != nil {
		c.logger.Error(
			"error committing offset",
			"offset", offset,
			"error", err,
		)
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
		c.logger.Info("ignoring internal events like partition assignments or rebalances")
	}

	return nil
}

func (c *KafkaConsumer) process(msg *kafka.Message) error {
	c.logger.Info("processing message",
		"topic", *msg.TopicPartition.Topic,
		"partition", msg.TopicPartition.Partition,
		"offset", msg.TopicPartition.Offset,
	)

	switch msg.Value {
	case nil:
		return c.remove(msg)
	default:
		return c.update(msg)
	}
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

	c.dataChannel <- c.updatePayload(&eventKey, &eventMesage, msg)
	c.logger.Info("message pushed in queue")
	return nil
}

func (c *KafkaConsumer) updatePayload(eventKey *v1.PBEventKey, eventMsg *v1.PBEventMessage, msg *kafka.Message) *Payload {
	return &Payload{
		Key: &Key{
			Id:        eventKey.EventId,
			Timestamp: time.Unix(eventKey.Timestamp, 0).UTC(),
		},
		Value: &Message{
			Id:         eventMsg.EventId,
			ResourceId: eventMsg.ResourceId,
			Type:       eventMsg.Type,
			Start:      time.Unix(eventMsg.Start, 0).UTC(),
			End:        time.Unix(eventMsg.End, 0).UTC().UTC(),
		},
		IsDelete:  false,
		Partition: msg.TopicPartition.Partition,
		Offset:    int64(msg.TopicPartition.Offset),
	}
}

func (c *KafkaConsumer) remove(msg *kafka.Message) error {
	var eventKey v1.PBEventKey
	if err := proto.Unmarshal(msg.Key, &eventKey); err != nil {
		return fmt.Errorf("failed to unmarshal protobuff payload 'msg.Key'")
	}

	c.dataChannel <- c.removePayload(&eventKey, msg)
	return nil
}

func (c *KafkaConsumer) removePayload(eventKey *v1.PBEventKey, msg *kafka.Message) *Payload {
	return &Payload{
		Key: &Key{
			Id:        eventKey.EventId,
			Timestamp: time.Unix(eventKey.Timestamp, 0).UTC(),
		},
		Value:     &Message{},
		IsDelete:  true,
		Partition: msg.TopicPartition.Partition,
		Offset:    int64(msg.TopicPartition.Offset),
	}
}

func (c *KafkaConsumer) close() {
	c.logger.Info("closing kafka connection")
	if err := c.kConsumer.Close(); err != nil {
		c.logger.Error("failed to close kafka consumer cleanly", "error", err)
	}
}
