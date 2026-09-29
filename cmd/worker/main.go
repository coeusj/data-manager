package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"local/data-manager/internal/configuration"
	"local/data-manager/internal/consumer"
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

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("starting background worker..")

	if err := start(ctx, logger); err != nil {
		return fmt.Errorf("worker execution error: %w", err)
	}

	return nil
}

func start(ctx context.Context, logger *slog.Logger) error {
	configPath := flag.String("config", "", "configuration's file path")
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "Error: -config flag is required")
		os.Exit(1)
	}

	config, err := configuration.LoadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("could not load configurations: %w", err)
	}

	consumer := consumer.New(config.Kafka, logger)
	if err := consumer.Start(ctx); err != nil {
		return fmt.Errorf("could not start consumer: %w", err)
	}

	return nil
}
