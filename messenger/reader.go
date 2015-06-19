package messenger

import (
	"github.com/andrew-suprun/envoy/actor"
	"net"
)

type reader struct {
	name string
	hostId
	actor.Actor
	net.Conn
	recipient actor.Actor
}

func newReader(name string, hostId hostId, conn net.Conn, recipient actor.Actor) actor.Actor {
	reader := &reader{
		name:      name,
		hostId:    hostId,
		Actor:     actor.NewActor(name),
		Conn:      conn,
		recipient: recipient,
	}

	reader.
		RegisterHandler("read-message", reader.handleReadMessage).
		Start().
		Send("read-message")
	return reader
}

func (reader *reader) handleReadMessage(_ string, _ []interface{}) {
	msg, err := readMessage(reader.Conn)
	if err != nil {
		reader.recipient.Send("network-error", reader.hostId, err)
	} else {
		reader.recipient.Send("message", reader.hostId, msg)
		reader.Send("read-message")
	}
}

func (reader *reader) logf(format string, params ...interface{}) {
	Log.Debugf(">>> %s: "+format, append([]interface{}{reader.name}, params...)...)
}
