package messenger

import (
	"github.com/andrew-suprun/envoy/actor"
	"net"
)

type reader struct {
	name string
	actor.Actor
	net.Conn
	recipient actor.Actor
}

func newReader(name string, conn net.Conn, recipient actor.Actor) actor.Actor {
	reader := &reader{
		name:      name,
		Actor:     actor.NewActor(name),
		Conn:      conn,
		recipient: recipient,
	}

	reader.
		RegisterHandler("read-message", reader.handleReadMessage).
		RegisterHandler("stop", reader.handleStop).
		Send("read-message")
	return reader
}

func (reader *reader) handleReadMessage(_ string, _ []interface{}) {
	msg, err := readMessage(reader.Conn)
	if err != nil {
		reader.recipient.Send("error", err)
	} else {
		reader.recipient.Send("read", msg)
		reader.Send("read-message")
	}
}

func (reader *reader) handleStop(_ string, _ []interface{}) {
	reader.Close()
}

func (reader *reader) logf(format string, params ...interface{}) {
	Log.Debugf(">>> %s: "+format, append([]interface{}{reader.name}, params...)...)
}
