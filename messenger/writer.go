package messenger

import (
	"github.com/andrew-suprun/envoy/actor"
	"net"
)

type writer struct {
	name string
	actor.Actor
	net.Conn
	recipient actor.Actor
	stopped   bool
}

func newWriter(name string, conn net.Conn, recipient actor.Actor) actor.Actor {
	writer := &writer{
		name:      name,
		Actor:     actor.NewActor(name),
		Conn:      conn,
		recipient: recipient,
	}

	return writer.
		RegisterHandler("write", writer.handleWrite).
		Start()
}

func (writer *writer) handleWrite(_ actor.MessageType, info actor.Payload) {
	err := writeMessage(writer.Conn, info.(*message))
	if err != nil {
		writer.recipient.Send("error", &errorMessage{writer.name, err})
		writer.Stop()
	}
}
