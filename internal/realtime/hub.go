package realtime

import (
	"context"
	"log/slog"
)

type Client struct {
	send chan []byte
}

func NewClient() *Client {
	return &Client{make(chan []byte)}
}

type Hub interface {
	Add(c *Client)
	Remove(c *Client)
}

type Broadcaster interface {
	Broadcast(ctx context.Context, msg []byte)
}

type WsHub struct {
	clients    []*Client
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	logger     *slog.Logger
}

func NewWsHub(logger *slog.Logger) *WsHub {
	return &WsHub{
		clients:    make([]*Client, 0),
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		logger:     logger,
	}
}

func (h *WsHub) Start(ctx context.Context) {
	h.logger.Info("Hub started")

	for {
		select {
		case <-ctx.Done():
			h.logger.Info("context cancelled. Closing Hub")
			return
		default:
			select {
			case c := <-h.register:
				h.clients = append(h.clients, c)
				h.logger.Info("client added")
			case c := <-h.unregister:
				for i, client := range h.clients {
					if client == c {
						h.clients = append(h.clients[:i], h.clients[i+1:]...)
						close(c.send)
						break
					}
				}
				h.logger.Info("client removed")
			case msg := <-h.broadcast:
				h.logger.Info("broadcasting message")
				for i, c := range h.clients {
					select {
					case c.send <- msg:
						h.logger.Info("message sent to client's channel", "index", i)
					default:
						h.logger.Warn("client's buffer could be full", "index", i)
						// if client's send buffer is full, drop or disconnect
					}
				}
			}
		}
	}
}

func (h *WsHub) Add(c *Client) {
	h.register <- c
}

func (h *WsHub) Remove(c *Client) {
	h.unregister <- c
}

func (h *WsHub) Broadcast(ctx context.Context, msg []byte) {
	h.broadcast <- msg
}
