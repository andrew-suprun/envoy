package reader

import (
	. "github.com/andrew-suprun/envoy"
	"github.com/andrew-suprun/envoy/actor"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"net"
)

type reader struct {
	actor.Actor
	name      string
	hostId    HostId
	conn      net.Conn
	recipient Sender
}

func NewReader(name string, hostId HostId, conn net.Conn, recipient Sender) Sender {
	reader := &reader{
		name:      name,
		hostId:    hostId,
		Actor:     actor.NewActor(name),
		conn:      conn,
		recipient: recipient,
	}

	reader.
		RegisterHandler("read-message", reader.handleReadMessage).
		RegisterHandler("leave", reader.handleLeave).
		Start().
		Send("read-message")
	return reader
}

func (reader *reader) handleReadMessage(_ string, _ []interface{}) {
	msg, err := ReadMessage(reader.conn)
	if err != nil {
		reader.recipient.Send("network-error", reader.hostId, err)
	} else {
		reader.recipient.Send("message", reader.hostId, msg)
		reader.Send("read-message")
	}
}

func (reader *reader) handleLeave(_ string, _ []interface{}) {
	// TODO: ?
	reader.Stop()
}

func (reader *reader) logf(format string, params ...interface{}) {
	Log.Debugf(">>> %s: "+format, append([]interface{}{reader.name}, params...)...)
}
