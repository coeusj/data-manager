package websocket

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type Hub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan []byte
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	logger     *slog.Logger
	mu         sync.Mutex
}

func NewHub(logger *slog.Logger) *Hub {
	return &Hub{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan []byte),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
		logger:     logger,
	}
}

func (h *Hub) Start(ctx context.Context) {
	h.logger.Info("Hub started")
	for {
		select {
		case conn := <-h.register:
			h.addConnection(conn)
		case conn := <-h.unregister:
			h.removeConnection(conn)
		case msg := <-h.broadcast:
			h.broadcastMsg(ctx, msg)
		}
	}
}

func (h *Hub) Register(conn *websocket.Conn) {
	h.register <- conn
}

func (h *Hub) UnRegister(conn *websocket.Conn) {
	h.register <- conn
}

func (h *Hub) Send(msg []byte) {
	h.broadcast <- msg
}

func (h *Hub) addConnection(conn *websocket.Conn) {
	h.mu.Lock()

	h.clients[conn] = true
	h.logger.Info("client registered")

	h.mu.Unlock()
}

func (h *Hub) removeConnection(conn *websocket.Conn) {
	h.mu.Lock()

	if _, ok := h.clients[conn]; ok {
		delete(h.clients, conn)
		conn.Close(websocket.StatusNormalClosure, "unregistered")
		h.logger.Info("client unregistered")
	}

	h.mu.Unlock()
}

// TODO: consider batch send
func (h *Hub) broadcastMsg(ctx context.Context, msg []byte) {
	h.logger.Info("received message to broadcast")
	h.mu.Lock()

	for conn := range h.clients {
		wCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		err := conn.Write(wCtx, websocket.MessageBinary, msg)
		cancel()
		if err != nil {
			conn.Close(websocket.StatusInternalError, "failed write")
			delete(h.clients, conn)
		}
	}

	h.logger.Info("message broadcasted")
	h.mu.Unlock()
}
