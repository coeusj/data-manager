package websocket

import (
	"log/slog"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

func NewWebsocketConnectionHandler(ctx *gin.Context, hub *Hub, logger *slog.Logger) {
	conn, err := websocket.Accept(ctx.Writer, ctx.Request, nil)
	if err != nil {
		logger.Error("error during handshake", "error", err)
		return
	}
	hub.Register(conn)
}
