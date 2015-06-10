package messenger

import (
	"github.com/andrew-suprun/envoy/actor"
	"net"
	"sync"
)

type client struct {
	name string
	actor.Actor
	hostId
	msgr     actor.Actor
	reader   actor.Actor
	writer   actor.Actor
	inflight sync.WaitGroup
	stopped  bool
}

func newClient(name string, clientId hostId, conn net.Conn, msgr actor.Actor) actor.Actor {
	client := &client{
		name:   name,
		hostId: clientId,
		Actor:  actor.NewActor(name),
		msgr:   msgr,
	}
	client.reader = newReader(client.name+"-reader", conn, client)
	client.writer = newWriter(client.name+"-writer", conn, client)

	return client.
		RegisterHandler("read", client.handleRead).
		RegisterHandler("write", client.handleWrite).
		RegisterHandler("error", client.handleError).
		Start()
}

func (client *client) handleRead(msgType actor.MessageType, info actor.Payload) {
	msg := info.(*actorMessage)
	if msg.error != nil {
		client.msgr.Send("error", &errorMessage{client.name, msg.error})
		client.stop()
	}

	client.msgr.Send("message", &messageCommand{info, client.writer})
}

func (client *client) handleWrite(msgType actor.MessageType, info actor.Payload) {
	client.writer.Send("write", info)
}

func (client *client) handleError(msgType actor.MessageType, info actor.Payload) {
	msg := info.(error)
	client.msgr.Send("error", &errorMessage{client.name, msg})
	client.stop()
}

func (client *client) stop() {
	client.writer.Stop()
	client.reader.Stop()
}
