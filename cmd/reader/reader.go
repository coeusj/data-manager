package main

import (
	"context"
	"fmt"
	"local/data-manager/internal/configuration"
	"local/data-manager/internal/consumer"
	"local/data-manager/internal/persistence"
	"local/data-manager/internal/websocket"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
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

	hub := websocket.NewHub(logger)
	go hub.Start(ctx)

	payloadChannel := make(chan *consumer.Payload, 5000)

	kafkaConsumer, err := consumer.NewKafkaConsumer(config.Kafka, payloadChannel, hub, logger)
	if err != nil {
		return err
	}
	go kafkaConsumer.Run(ctx)

	cache, err := persistence.NewRedisCache(ctx, config.Redis, logger)
	if err != nil {
		return fmt.Errorf("could not create cache instance %w", err)
	}

	serviceWorker := persistence.NewServiceWorker(cache, hub, kafkaConsumer.Commit, logger)
	go serviceWorker.Start(ctx, payloadChannel)

	router := gin.Default()
	router.GET("/rt-events", func(ctx *gin.Context) {
		websocket.NewWebsocketConnectionHandler(ctx, hub, logger)
	})

	router.Run(":8989")

	close(payloadChannel)
	return nil
}
