package producer

import (
	"context"
	"fmt"
	"local/data-manager/internal/configuration"
	"log/slog"
	"strings"
	"sync"

	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	kProducer sarama.AsyncProducer
	logger    *slog.Logger
}

func NewKafkaProducer(kafkaConfig configuration.KafkaConfig, conf configuration.ProducerConfig, logger *slog.Logger) (*KafkaProducer, error) {
	kConfig := sarama.NewConfig()
	kConfig.Producer.RequiredAcks = sarama.WaitForAll
	kConfig.Producer.Retry.Max = 5
	kConfig.Producer.Return.Successes = true
	kConfig.Producer.Partitioner = sarama.NewHashPartitioner
	kConfig.Producer.Compression = sarama.CompressionZSTD

	kProducer, err := sarama.NewAsyncProducer(strings.Split(kafkaConfig.Brokers, ","), kConfig)
	if err != nil {
		return nil, fmt.Errorf("could not create kafka producer: %w", err)
	}

	return &KafkaProducer{
		kProducer: kProducer,
		logger:    logger,
	}, nil
}

func (k *KafkaProducer) Start(ctx context.Context, wg *sync.WaitGroup) error {
	k.logger.Info("producer started")
	return nil
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
	default:
		return nil
	}
}

func (k *KafkaProducer) Stop() {
	k.kProducer.Close()
}
