package main

import (
	"context"
	v1 "local/data-manager/gen/go/event/v1"
	"local/data-manager/internal/configuration"
	"local/data-manager/internal/producer"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("publisher starting")

	config, err := configuration.LoadPublisherConfig("config.pub.json")
	if err != nil {
		logger.Error("could not find configuration")
		os.Exit(1)
	}

	var ctx = context.Background()

	kProducer, err := producer.NewKafkaProducer(config.Kafka, logger)
	if err != nil {
		logger.Error("could not create kafka producer")
		os.Exit(1)
	}
	defer kProducer.Close()

	duration := time.Second * time.Duration(config.Sender.DurationSeconds)
	deadline := time.Now().Add(duration)
	ticker := time.NewTicker(time.Millisecond * time.Duration(config.Sender.TickMs))

	defer ticker.Stop()
	count := 0
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			logger.Warn("context cancelled")
			os.Exit(1)
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
				Topic: config.Kafka.Topic,
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
}
