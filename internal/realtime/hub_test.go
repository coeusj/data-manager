package realtime

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestHub_AddNewClient(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	hub := NewWsHub(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hub.Start(ctx)

	client := NewClient()

	hub.Add(client)

	time.Sleep(10 * time.Millisecond)

	testMsg := []byte("test-msg")
	hub.Broadcast(ctx, testMsg)

	select {
	case msg := <-client.send:
		if string(msg) != string(testMsg) {
			t.Errorf("expected message %q, got %q", testMsg, msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for broadcast message on client channel")
	}

	hub.Remove(client)
	time.Sleep(10 * time.Millisecond)
}

func TestHub_RemoveClient(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	hub := NewWsHub(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hub.Start(ctx)

	client := NewClient()

	hub.Add(client)
	time.Sleep(10 * time.Millisecond)

	hub.Remove(client)
	time.Sleep(10 * time.Millisecond)

	testMsg := []byte("test-msg")
	hub.Broadcast(ctx, testMsg)

	select {
	case _, ok := <-client.send:
		if ok {
			t.Errorf("client's channel still open, expected to be closed")
		}
	case <-time.After(1 * time.Second):
		break
	}
}

func TestHub_BroadcastMessageToAllClients(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	hub := NewWsHub(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hub.Start(ctx)

	clients := make([]*Client, 0)
	clients = append(clients, NewClient())
	clients = append(clients, NewClient())
	for _, c := range clients {
		hub.Add(c)
	}
	time.Sleep(10 * time.Millisecond)

	testMsg := []byte("test-msg")
	hub.Broadcast(ctx, testMsg)

	for i, c := range clients {
		select {
		case msg := <-c.send:
			if string(msg) != string(testMsg) {
				t.Errorf("expected message %q, got %q", testMsg, msg)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("timed out waiting for broadcast message on client's %d channel", i)
		}
	}

	for _, c := range clients {
		hub.Remove(c)
	}
	time.Sleep(10 * time.Millisecond)
}
