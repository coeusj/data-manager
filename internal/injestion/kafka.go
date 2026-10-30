package injestion

import (
	"context"
	"crypto/sha256"
	"local/data-manager/internal/configuration"
	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Deserializer[T any] func([]byte) (*T, error)

type KafkaConsumer[K any, V any] struct {
	kConsumer      *kafka.Consumer
	topic          *string
	pollTimeoutMs  int
	messages       chan *Payload[K, V]
	deserializeKey Deserializer[K]
	deserializeVal Deserializer[V]
	logger         *slog.Logger
}

func NewKafkaConsumer[K any, V any](
	kafkaConfig configuration.KafkaConfig,
	deserializeKey func([]byte) (*K, error),
	deserializeVal func([]byte) (*V, error),
	logger *slog.Logger) (*KafkaConsumer[K, V], error) {
	consumerConfig := kafka.ConfigMap{
		"bootstrap.servers":  kafkaConfig.Brokers,
		"group.id":           kafkaConfig.GroupID,
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

	return &KafkaConsumer[K, V]{
		kConsumer:      consumer,
		topic:          &kafkaConfig.Topic,
		pollTimeoutMs:  100, // Poll for events with a 100ms timeout to keep loop responsive to ctx.Done()
		messages:       make(chan *Payload[K, V], 500),
		deserializeKey: deserializeKey,
		deserializeVal: deserializeVal,
		logger:         logger,
	}, nil
}

func (k *KafkaConsumer[K, V]) Run(ctx context.Context) {
	defer k.close()

	k.logger.Info("consume loop starting")

	for {
		select {
		case <-ctx.Done():
			k.logger.Warn("context canceled, exiting consumer loop")
			return
		default:
			event := k.kConsumer.Poll(k.pollTimeoutMs)
			if event == nil {
				continue
			}

			if err := k.consume(event); err != nil {
				k.logger.Error("failed to consume event", "error", err)
				continue
			}
		}
	}
}

func (k *KafkaConsumer[K, V]) Messages() chan *Payload[K, V] {
	return k.messages
}

func (k *KafkaConsumer[K, V]) Commit(partition int32, offset int64) {
	offsetsToCommit := []kafka.TopicPartition{
		{
			Topic:     k.topic,
			Partition: partition,
			Offset:    kafka.Offset(offset + 1),
		},
	}

	_, err := k.kConsumer.CommitOffsets(offsetsToCommit)
	if err != nil {
		k.logger.Error(
			"error committing offset",
			"offset", offset,
			"error", err,
		)
	}

	k.logger.Info(
		"offset commited",
		"topic", k.topic,
		"partition", partition,
		"offset", offset,
	)
}

func (k *KafkaConsumer[K, V]) consume(event kafka.Event) error {
	switch e := event.(type) {
	case *kafka.Message:
		if err := k.processEvent(e); err != nil {
			k.logger.Error("failed to process message",
				"topic", *e.TopicPartition.Topic,
				"partition", e.TopicPartition.Partition,
				"offset", e.TopicPartition.Offset,
				"error", err,
			)
			return err
		}
	case kafka.Error:
		if e.IsFatal() {
			k.logger.Error("fatal kafka client error", "error", e)
			return e
		}
		k.logger.Warn("non-fatal kafka error encountered", "error", e)
		return e
	default:
		k.logger.Info("ignoring internal events like partition assignments or rebalances")
	}

	return nil
}

func (k *KafkaConsumer[K, V]) processEvent(msg *kafka.Message) error {
	key, err := k.deserializeKey(msg.Key)
	if err != nil {
		return err
	}

	k.logger.Info("processing message",
		"topic", *msg.TopicPartition.Topic,
		"partition", msg.TopicPartition.Partition,
		"offset", msg.TopicPartition.Offset,
	)

	switch msg.Value {
	case nil:
		k.messages <- &Payload[K, V]{
			HashKey:   sha256.Sum256(msg.Key),
			Key:       key,
			IsDelete:  true,
			Partition: msg.TopicPartition.Partition,
			Offset:    int64(msg.TopicPartition.Offset),
		}
	default:
		val, err := k.deserializeVal(msg.Value)
		if err != nil {
			return err
		}

		k.messages <- &Payload[K, V]{
			HashKey:   sha256.Sum256(msg.Key),
			Key:       key,
			Value:     val,
			IsDelete:  false,
			Partition: msg.TopicPartition.Partition,
			Offset:    int64(msg.TopicPartition.Offset),
		}
	}

	return nil
}

func (k *KafkaConsumer[K, V]) close() {
	k.logger.Info("closing kafka connection")
	if err := k.kConsumer.Close(); err != nil {
		k.logger.Error("failed to close kafka consumer cleanly", "error", err)
	}
}
