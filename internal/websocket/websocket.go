package websocket

import (
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // allow all origin for testing
	},
}

func ServeWebSocket(hub *Hub, w http.ResponseWriter, r *http.Request, logger *slog.Logger) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("error upgrading connection", "error", err)
		return
	}

	client := &Client{
		hub:        hub,
		connection: conn,
		send:       make(chan []byte, 256),
	}
	hub.register <- client

	go client.write()
}
