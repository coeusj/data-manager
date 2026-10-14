package producer

import (
	"context"
	"fmt"
	"local/data-manager/internal/configuration"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type KafkaBatchProducer struct {
	kProducer           sarama.SyncProducer
	batchSize           int
	channel             chan *sarama.ProducerMessage
	autoSendFrequencyMs time.Duration
	lastUpdate          time.Time
	started             bool
	logger              *slog.Logger
}

func NewKafkaBatchProducer(kafkaConfig configuration.KafkaConfig, conf configuration.ProducerConfig, logger *slog.Logger) (*KafkaBatchProducer, error) {
	kConfig := sarama.NewConfig()
	kConfig.Producer.RequiredAcks = sarama.WaitForAll
	kConfig.Producer.Retry.Max = 5
	kConfig.Producer.Return.Successes = true
	kConfig.Producer.Partitioner = sarama.NewHashPartitioner
	kConfig.Producer.Compression = sarama.CompressionZSTD

	kProducer, err := sarama.NewSyncProducer(strings.Split(kafkaConfig.Brokers, ","), kConfig)
	if err != nil {
		return nil, fmt.Errorf("could not create kafka producer: %w", err)
	}

	return &KafkaBatchProducer{
		kProducer:           kProducer,
		batchSize:           conf.BatchSize,
		channel:             make(chan *sarama.ProducerMessage, 1000),
		autoSendFrequencyMs: time.Duration(conf.FlushFrequencyMs),
		started:             false,
		logger:              logger,
	}, nil
}

func (k *KafkaBatchProducer) Start(ctx context.Context, wg *sync.WaitGroup) error {
	if k.started {
		k.logger.Warn("producer already started")
		return nil
	}

	wg.Add(1)
	k.started = true

	go func() {
		defer func() {
			k.started = false
			wg.Done()
		}()

		buffer := make([]*sarama.ProducerMessage, 0, k.batchSize)
		ticker := time.NewTicker(k.autoSendFrequencyMs * time.Millisecond)
		defer ticker.Stop()

		flush := func() {
			if len(buffer) == 0 {
				return
			}

			err := k.kProducer.SendMessages(buffer)
			if err != nil {
				k.logger.Error("failed to publish message batch", "error", err)
			} else {
				k.logger.Info("send messages batch", "count", len(buffer))
			}
			buffer = buffer[:0]
		}

		for {
			select {
			case <-ctx.Done():
				flush()
				return
			case msg, ok := <-k.channel:
				if !ok {
					k.logger.Warn("Channel closed. flush remaining messages and exit")
					flush()
					return
				}

				buffer = append(buffer, msg)
				if len(buffer) >= k.batchSize {
					k.logger.Info("batch size reached. sending messages")
					flush()
				}
			case <-ticker.C:
				if len(buffer) > 0 {
					k.logger.Info("periodic automatic flush after", "ms", k.autoSendFrequencyMs)
					flush()
				}
			}
		}
	}()

	return nil
}

func (k *KafkaBatchProducer) Send(ctx context.Context, msg *sarama.ProducerMessage) error {
	k.channel <- msg
	k.lastUpdate = time.Now().UTC()
	return nil
}

func (k *KafkaBatchProducer) Stop() {
	k.started = false
	k.kProducer.Close()
}
