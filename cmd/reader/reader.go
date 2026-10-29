package main

import (
	"context"
	"fmt"
	v1 "local/data-manager/gen/go/event/v1"
	"local/data-manager/internal/configuration"
	"local/data-manager/internal/consumer"
	"local/data-manager/internal/persistence"
	"local/data-manager/internal/realtime"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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

	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	logger.Info("starting reader")

	if err := start(ctx, logger); err != nil {
		return fmt.Errorf("worker execution error: %w", err)
	}

	return nil
}

func start(ctx context.Context, logger *slog.Logger) error {
	configPath := os.Getenv("DM_CONFIG_PATH")
	if configPath == "" {
		logger.Error("missing env variable DM_CONFIG_PATH")
		os.Exit(1)
	}

	config, err := configuration.LoadWorkerConfig(configPath)
	if err != nil {
		return fmt.Errorf("could not load configurations: %w", err)
	}

	hub := realtime.NewWsHub(logger)
	go hub.Start(ctx)

	cache, err := persistence.NewRedisCache(ctx, config.Redis, logger)
	if err != nil {
		return fmt.Errorf("could not create cache instance %w", err)
	}

	deserKey := func(key []byte) (*v1.PBEventKey, error) {
		pbKey := &v1.PBEventKey{}
		if err := proto.Unmarshal(key, pbKey); err != nil {
			return nil, err
		}
		return pbKey, nil
	}
	deserVal := func(val []byte) (*v1.PBEventMessage, error) {
		pbVal := &v1.PBEventMessage{}
		if err := proto.Unmarshal(val, pbVal); err != nil {
			return nil, err
		}
		return pbVal, nil
	}

	consumer, err := consumer.NewKafkaConsumer(config.Kafka, deserKey, deserVal, logger)
	if err != nil {
		return err
	}

	serviceWorker := persistence.NewServiceWorker[v1.PBEventKey, v1.PBEventMessage](cache, hub, consumer.Commit, logger)
	go serviceWorker.Start(ctx, consumer.Messages())

	go consumer.Run(ctx)

	router := gin.Default()
	router.GET("/events", func(ctx *gin.Context) {
		realtime.NewWebsocketConnectionHandler(ctx, hub, logger)
	})

	router.Run(":8989")

	cache.Close()
	return nil
}
