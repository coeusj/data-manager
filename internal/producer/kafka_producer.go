package producer

import (
	"context"
	"fmt"
	"local/data-manager/internal/configuration"
	"log/slog"
	"strings"

	"github.com/IBM/sarama"
)

// NOTE: using IBM Sarama(Kafka) just for simplicity and testing purpose
type KafkaProducer struct {
	kProducer sarama.AsyncProducer
	logger    *slog.Logger
}

func NewKafkaProducer(kafkaConfig configuration.KafkaConfig, logger *slog.Logger) (*KafkaProducer, error) {
	kConfig := sarama.NewConfig()
	kConfig.Producer.RequiredAcks = sarama.WaitForAll
	kConfig.Producer.Retry.Max = 5
	kConfig.Producer.Return.Successes = true
	kConfig.Producer.Partitioner = sarama.NewHashPartitioner

	kProducer, err := sarama.NewAsyncProducer(strings.Split(kafkaConfig.Brokers, ","), kConfig)
	if err != nil {
		return nil, fmt.Errorf("could not create kafka producer: %w", err)
	}

	return &KafkaProducer{
		kProducer: kProducer,
		logger:    logger,
	}, nil
}

func (k *KafkaProducer) Send(ctx context.Context, msg *sarama.ProducerMessage) error {
	k.kProducer.Input() <- msg

	select {
	case success := <-k.kProducer.Successes():
		k.logger.Info("data sent", "partition", success.Partition, "offset", success.Offset)
		return nil
	case err := <-k.kProducer.Errors():
		k.logger.Error("error while trying to send message", "error", err)
		return err
	case <-ctx.Done():
		return fmt.Errorf("operation cancelled while sending message")
	}
}

func (k *KafkaProducer) Close() {
	k.kProducer.Close()
}
