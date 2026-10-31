package main

import (
	"context"
	"fmt"
	v1 "local/data-manager/gen/go/event/v1"
	"local/data-manager/internal/configuration"
	"local/data-manager/internal/injestion"
	"local/data-manager/internal/realtime"
	"local/data-manager/internal/storage"
	"local/data-manager/internal/worker"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"uuid"

	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/proto"
)

func main() {
	if err := run(); err != nil {
		slog.Error("reader process failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	logger.Info("starting reader")

	if err := start(ctx, logger); err != nil {
		return fmt.Errorf("worker execution error: %w", err)
	}

	return nil
}

func start(ctx context.Context, logger *slog.Logger) error {
	if err := configuration.LoadEnvFile(logger); err != nil {
		logger.Error("could not load env file", "error", err)
	}

	config, err := configuration.LoadReaderConfig(os.Getenv("CONF_PATH"))
	if err != nil {
		logger.Error("could not load configuration file", "error", err)
		os.Exit(1)
	}

	hub := realtime.NewWsHub(logger)
	cache := storage.NewEventsCache(ctx, config, logger)
	consumer := injestion.NewEventsConsumer(config, decodeKey, decodeVal, generateID, logger)
	serviceWorker := worker.NewServiceWorker[v1.PBEventKey, v1.PBEventMessage](cache, hub, consumer.Commit, logger)

	go hub.Start(ctx)
	go serviceWorker.Start(ctx, consumer.Messages())
	go consumer.Run(ctx)

	router := gin.Default()
	router.GET("/events", func(ctx *gin.Context) {
		realtime.EventsHandler(ctx, hub, logger)
	})

	router.Run(os.Getenv("READER_URL"))

	cache.Close()
	return nil
}

func decodeKey(key []byte) (*v1.PBEventKey, error) {
	pbKey := &v1.PBEventKey{}
	if err := proto.Unmarshal(key, pbKey); err != nil {
		return nil, err
	}

	return pbKey, nil
}

func decodeVal(val []byte) (*v1.PBEventMessage, error) {
	pbVal := &v1.PBEventMessage{}
	if err := proto.Unmarshal(val, pbVal); err != nil {
		return nil, err
	}

	return pbVal, nil
}

func generateID(key []byte) string {
	pbKey := &v1.PBEventKey{}
	if err := proto.Unmarshal(key, pbKey); err != nil {
		randomID := uuid.New()
		return randomID.String()
	}

	return fmt.Sprintf("%s:%d", pbKey.EventId, pbKey.Timestamp)
}
