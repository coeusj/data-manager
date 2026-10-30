package realtime

import (
	"log/slog"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

func EventsHandler(ctx *gin.Context, hub Hub, logger *slog.Logger) {
	conn, err := websocket.Accept(ctx.Writer, ctx.Request, nil)
	if err != nil {
		logger.Error("error during handshake", "error", err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "context cancelled")

	client := NewClient()
	hub.Add(client)

	for {
		select {
		case msg, ok := <-client.Messages():
			if !ok {
				continue
			}

			conn.Write(ctx, websocket.MessageBinary, msg)
		case <-ctx.Done():
			logger.Warn("context cancelled. exiting")
			return
		}
	}
}
