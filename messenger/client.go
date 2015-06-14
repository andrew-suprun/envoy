package messenger

import (
	"fmt"
	"github.com/andrew-suprun/envoy/actor"
	"net"
	"sync"
)

type client struct {
	name string
	actor.Actor
	clientId hostId
	msgr     actor.Actor
	reader   actor.Actor
	writer   actor.Actor
	inflight sync.WaitGroup
}

func (h *client) String() string {
	return fmt.Sprintf("[client %s]", h.name)
}

func newClient(name string, clientId hostId, conn net.Conn, msgr actor.Actor) actor.Actor {
	client := &client{
		name:     name,
		clientId: clientId,
		Actor:    actor.NewActor(name),
		msgr:     msgr,
	}
	client.reader = newReader(client.name+"-reader", conn, client)
	client.writer = newWriter(client.name+"-writer", conn, client)

	return client.
		RegisterHandler("read", client.handleRead).
		RegisterHandler("write", client.handleWrite).
		RegisterHandler("error", client.handleError).
		RegisterHandler("stop", client.handleStop)
}

func (client *client) handleRead(msgType string, info []interface{}) {
	msg := info[0].(*message)
	client.msgr.Send("message", client.writer, msg)
}

func (client *client) handleError(msgType string, info []interface{}) {
	err := info[0].(error)
	Log.Errorf("%s: Received network error: %v. Will try to re-connect.", client.name, err)
	client.msgr.Send("client-error", client.clientId, err)
}

func (client *client) handleWrite(msgType string, info []interface{}) {
	client.writer.Send("write", info...)
}

func (client *client) handleStop(msgType string, _ []interface{}) {
	client.writer.Send("stop")
	client.reader.Send("stop")
}
