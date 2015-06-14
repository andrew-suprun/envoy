package messenger

import (
	"github.com/andrew-suprun/envoy/actor"
	"net"
)

type writer struct {
	name string
	hostId
	actor.Actor
	net.Conn
	recipient actor.Actor
}

func newWriter(name string, hostId hostId, conn net.Conn, recipient actor.Actor) actor.Actor {
	writer := &writer{
		name:      name,
		hostId:    hostId,
		Actor:     actor.NewActor(name),
		Conn:      conn,
		recipient: recipient,
	}

	return writer.
		RegisterHandler("write", writer.handleWrite).
		RegisterHandler("stop", writer.handleStop)
}

func (writer *writer) handleWrite(_ string, info []interface{}) {
	err := writeMessage(writer.Conn, info[0].(*message))
	if err != nil {
		writer.recipient.Send("network-error", writer.hostId, err)
	}
}

func (writer *writer) handleStop(_ string, _ []interface{}) {
	writer.Close()
}

func (writer *writer) logf(format string, params ...interface{}) {
	Log.Debugf(">>> %s: "+format, append([]interface{}{writer.name}, params...)...)
}
