package producer

import (
	"context"
	"local/data-manager/internal/configuration"
	"log/slog"
	"sync"

	"github.com/IBM/sarama"
)

// NOTE: using IBM Sarama(Kafka) just for simplicity and testing purpose
type Producer interface {
	Start(ctx context.Context, wg *sync.WaitGroup) error
	Send(ctx context.Context, msg *sarama.ProducerMessage) error
	Stop()
}

func NewProducer(batch bool, brokers string, conf *configuration.PublisherConfig, logger *slog.Logger) (Producer, error) {
	if batch {
		producerBatch, err := NewKafkaBatchProducer(brokers, conf, logger)
		if err != nil {
			return nil, err
		}
		return producerBatch, nil
	}

	producer, err := NewKafkaProducer(brokers, logger)
	if err != nil {
		return nil, err
	}
	return producer, nil
}
