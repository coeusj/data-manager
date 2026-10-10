package main

import (
	"context"
	"fmt"
	"local/data-manager/internal/configuration"
	"local/data-manager/internal/injestion"
	"local/data-manager/internal/persistence"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Worker process failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts := &slog.HandlerOptions{
		AddSource: true,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	logger.Info("starting background worker..")

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

	dataChannel := make(chan *injestion.Payload, 5000)

	storage, err := persistence.NewRedisStorage(ctx, config.Redis, logger)
	if err != nil {
		return fmt.Errorf("could not create storage instance %w", err)
	}

	backgroundWorker := persistence.NewBackgroundWorker(storage, logger)
	go backgroundWorker.Start(ctx, dataChannel)

	kafkaConsumer, err := injestion.NewKafkaConsumer(config.Kafka, dataChannel, logger)
	if err != nil {
		return err
	}

	if err := kafkaConsumer.Start(ctx); err != nil {
		return fmt.Errorf("could not start consumer: %w", err)
	}

	close(dataChannel)
	return nil
}
