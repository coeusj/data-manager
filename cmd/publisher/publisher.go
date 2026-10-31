package main

import (
	"context"
	v1 "local/data-manager/gen/go/event/v1"
	"local/data-manager/internal/configuration"
	"local/data-manager/internal/producer"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("publisher starting")

	brokers := "localhost:9092"
	if err := configuration.LoadEnvFile(logger); err != nil {
		logger.Error("could not load env file", "error", err)
	}

	config, err := configuration.LoadPublisherConfig("./config.publisher.json")
	if err != nil {
		logger.Error("could not load configuration file", "error", err)
		os.Exit(1)
	}

	duration := time.Second * time.Duration(config.Producer.DurationSeconds)
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	kProducer, err := producer.NewProducer(config.Producer.UseBatch, brokers, config, logger)
	if err != nil {
		logger.Error("could not create kafka producer")
		os.Exit(1)
	}
	wg := &sync.WaitGroup{}
	kProducer.Start(ctx, wg)
	defer kProducer.Stop()

	ticker := time.NewTicker(time.Millisecond * time.Duration(config.Producer.TickMs))
	defer ticker.Stop()
	count := 0

generationLoop:
	for {
		select {
		case <-ctx.Done():
			logger.Warn("generation loop done")
			break generationLoop
		case <-ticker.C:
			keyProto, err := proto.Marshal(&v1.PBEventKey{
				EventId:   "event-" + strconv.Itoa(count),
				Timestamp: time.Now().Unix(),
			})
			if err != nil {
				logger.Error("could not marshal message key", "error", err)
				continue
			}

			valueProto, err := proto.Marshal(&v1.PBEventMessage{
				EventId:    "event-" + strconv.Itoa(count),
				ResourceId: "resource-id",
				Type:       "random",
				Start:      time.Now().Unix(),
				End:        time.Now().Unix(),
			})
			if err != nil {
				logger.Error("could not marshal message value", "error", err)
				continue
			}

			msg := &sarama.ProducerMessage{
				Topic: config.Producer.Topic,
				Key:   sarama.ByteEncoder(keyProto),
				Value: sarama.ByteEncoder(valueProto),
			}

			if err := kProducer.Send(ctx, msg); err != nil {
				logger.Error("could not send message", "error", err)
				continue
			}
			count++
		}
	}

	wg.Wait()
	kProducer.Stop()
}
