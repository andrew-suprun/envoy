package writer

import (
	. "github.com/andrew-suprun/envoy"
	"github.com/andrew-suprun/envoy/actor"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"net"
)

type writer struct {
	actor.Actor
	name      string
	hostId    HostId
	conn      net.Conn
	recipient Sender
}

func NewWriter(name string, hostId HostId, conn net.Conn, recipient Sender) Sender {
	writer := &writer{
		name:      name,
		hostId:    hostId,
		Actor:     actor.NewActor(name),
		conn:      conn,
		recipient: recipient,
	}

	return writer.
		RegisterHandler("write", writer.handleWrite).
		RegisterHandler("leave", writer.handleLeave).
		Start()
}

func (writer *writer) handleWrite(_ string, info []interface{}) {
	msg := info[0].(*Message)
	err := WriteMessage(writer.conn, msg)
	writer.recipient.Send("write-result", writer.hostId, msg)
	if err != nil {
		writer.recipient.Send("network-error", writer.hostId, err)
	}
}

func (writer *writer) handleLeave(_ string, _ []interface{}) {
	// TODO: ?
	writer.Stop()
}

func (writer *writer) logf(format string, params ...interface{}) {
	Log.Debugf(">>> %s: "+format, append([]interface{}{writer.name}, params...)...)
}
