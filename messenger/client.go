package messenger

import (
	"github.com/andrew-suprun/envoy/actor"
	"net"
	"sync"
)

type client struct {
	actor.Actor
	hostId
	msgr     actor.Actor
	reader   actor.Actor
	writer   actor.Actor
	inflight sync.WaitGroup
	stopped  bool
}

func newClient(clientId hostId, conn net.Conn) actor.Actor {
	client := &client{
		hostId: clientId,
	}
	client.reader = newReader(conn, client)
	client.writer = newWriter(conn, client)

	return client.
		RegisterHandler("message", client.handleMessage).
		RegisterHandler("stop", client.handleStop).
		Start()
}

func (client *client) handleMessage(msgType actor.MessageType, info actor.Payload) {
	msg := info.(*actorMessage)
	if msg.error != nil {
		// todo: handle read error
	}

	client.msgr.Send("message", &messageCommand{info, client.writer})
}

func (client *client) handleStop(_ actor.MessageType, _ actor.Payload) {
}
