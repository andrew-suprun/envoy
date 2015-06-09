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

func newClient(clientId hostId, conn net.Conn, msgr actor.Actor) actor.Actor {
	client := &client{
		hostId: clientId,
		Actor:  actor.NewActor("client-" + string(clientId)),
		msgr:   msgr,
	}
	client.reader = newReader("client-reader-"+string(clientId), conn, client)
	client.writer = newWriter("client-writer-"+string(clientId), conn, client)

	return client.
		RegisterHandler("message", client.handleMessage).
		Start()
}

func (client *client) handleMessage(msgType actor.MessageType, info actor.Payload) {
	msg := info.(*actorMessage)
	if msg.error != nil {
		// todo: handle read error
	}

	client.msgr.Send("message", &messageCommand{info, client.writer})
}
