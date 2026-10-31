package injestion

import (
	v1 "local/data-manager/gen/go/event/v1"
	"local/data-manager/internal/configuration"
	"log"
	"log/slog"
	"os"
)

func NewEventsConsumer(
	conf *configuration.ReaderConfig,
	decodeKey Deserializer[v1.PBEventKey],
	decodeVal Deserializer[v1.PBEventMessage],
	generateID IDGenerator,
	logger *slog.Logger) *KafkaConsumer[v1.PBEventKey, v1.PBEventMessage] {
	brokers := os.Getenv("KAFKA_BROKERS")
	consumer, err := NewKafkaConsumer(brokers, conf, decodeKey, decodeVal, generateID, logger)
	if err != nil {
		log.Fatalf("could not create instance of kafka consumer: %v", err)
	}

	return consumer
}
