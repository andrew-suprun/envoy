package messenger

import (
	"github.com/andrew-suprun/envoy/actor"
	"net"
)

type writer struct {
	actor.Actor
	conn      *net.Conn
	recipient actor.Actor
	stopped   bool
}

func newWriter(conn net.Conn, recipient actor.Actor) actor.Actor {
	writer := &writer{
		writer:    actor.NewActor("writer"),
		conn:      conn,
		recipient: recipient,
	}

	return writer.
		RegisterHandler("message", writer.handleMessage).
		RegisterHandler("stop", writer.handleStop).
		Start()
}

func (writer *writer) handleMessage(_ actor.MessageType, info actor.Payload) {
	writeMessage(writer.conn, info.(*message))
}

func (writer *writer) handleStop(_ actor.MessageType, _ actor.Payload) {
	if !writer.stopped {
		writer.stopped = true
		writer.Stop()
	}
}

func (writer *writer) run() {
	for !writer.stopped {
		msg, err := readMessage(writer.conn)

		writer.recipientSend("message", &actorMessage{msg, err})

		if err != nil {
			writer.stop()
		}
	}
}
