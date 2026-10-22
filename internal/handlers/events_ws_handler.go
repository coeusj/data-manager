package handlers

import (
	"local/data-manager/internal/websocket"
	"log/slog"

	ws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

func NewWebsocketConnectionHandler(ctx *gin.Context, hub *websocket.Hub, logger *slog.Logger) {
	conn, err := ws.Accept(ctx.Writer, ctx.Request, nil)
	if err != nil {
		logger.Error("error during handshake", "error", err)
		return
	}
	hub.Register(conn)
}
