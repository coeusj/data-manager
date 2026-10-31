package configuration

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/joho/godotenv"
)

func LoadEnvFile(logger *slog.Logger, paths ...string) error {
	appEnv := os.Getenv("APP_ENV")
	if appEnv == "" {
		appEnv = "development"
	}

	if appEnv == "development" || appEnv == "debug" {
		logger.Info("Loading .env file for local development...")

		if len(paths) > 0 {
			if err := godotenv.Load(paths...); err != nil {
				return fmt.Errorf("Warning: .env file not found in path, relying on system environment variables: %v", err)
			}
		}

		if err := godotenv.Load(); err != nil {
			return fmt.Errorf("Warning: .env file not found, relying on system environment variables: %v", err)
		}
	}

	return nil
}
