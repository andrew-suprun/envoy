package messenger

import (
	"github.com/andrew-suprun/envoy/actor"
	"net"
)

type writer struct {
	actor.Actor
	net.Conn
	recipient actor.Actor
	stopped   bool
}

func newWriter(name string, conn net.Conn, recipient actor.Actor) actor.Actor {
	writer := &writer{
		Actor:     actor.NewActor(name),
		Conn:      conn,
		recipient: recipient,
	}

	return writer.
		RegisterHandler("message", writer.handleMessage).
		RegisterHandler("stop", writer.handleStop).
		Start()
}

func (writer *writer) handleMessage(_ actor.MessageType, info actor.Payload) {
	writeMessage(writer.Conn, info.(*message))
}

func (writer *writer) handleStop(_ actor.MessageType, _ actor.Payload) {
	if !writer.stopped {
		writer.stopped = true
		writer.Stop()
	}
}
