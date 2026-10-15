package websocket

import "github.com/gorilla/websocket"

type Client struct {
	hub        *Hub
	connection *websocket.Conn
	send       chan []byte
}

func (c *Client) write() {
	defer c.connection.Close()
	for {
		msg, ok := <-c.send
		if !ok {
			c.connection.WriteMessage(websocket.CloseMessage, []byte{})
			return
		}

		w, err := c.connection.NextWriter(websocket.TextMessage)
		if err != nil {
			return
		}
		w.Write(msg)

		nMsgs := len(c.send)
		for i := 0; i < nMsgs; i++ {
			w.Write([]byte{'\n'})
			w.Write(<-c.send)
		}

		if err := w.Close(); err != nil {
			return
		}
	}
}
