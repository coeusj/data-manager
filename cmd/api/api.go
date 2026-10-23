package main

import (
	"local/data-manager/internal/api"
	"log/slog"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("api starting")

	router := gin.Default()
	router.GET("/events", func(ctx *gin.Context) {
		res, err := api.GetEventsHandler(ctx, logger)
		if err != nil {
			logger.Error("error while processing request", "error", err)
		}
		ctx.JSON(http.StatusOK, res)
	})

	router.Run(":8080")
}
