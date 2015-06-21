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
	msgr actor.Actor
}

func newWriter(name string, hostId hostId, conn net.Conn, msgr actor.Actor) actor.Actor {
	writer := &writer{
		name:   name,
		hostId: hostId,
		Actor:  actor.NewActor(name),
		Conn:   conn,
		msgr:   msgr,
	}

	return writer.
		RegisterHandler("write", writer.handleWrite).
		Start()
}

func (writer *writer) handleWrite(_ string, info []interface{}) {
	msg := info[0].(*message)
	err := writeMessage(writer.Conn, msg)
	writer.msgr.Send("write-result", writer.hostId, msg, err)
}

func (writer *writer) logf(format string, params ...interface{}) {
	Log.Debugf(">>> %s: "+format, append([]interface{}{writer.name}, params...)...)
}
