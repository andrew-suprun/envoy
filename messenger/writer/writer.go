package writer

import (
	"bytes"
	"fmt"
	. "github.com/andrew-suprun/envoy"
	"github.com/andrew-suprun/envoy/actor"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"net"
)

type (
	MsgMessageWritten struct {
		HostId HostId
		Msg    *Message
	}
)

type writer struct {
	hostId    HostId
	conn      net.Conn
	recipient actor.Actor
}

func NewWriter(hostId HostId, conn net.Conn, recipient actor.Actor) actor.Actor {
	return actor.NewActor(&writer{
		hostId:    hostId,
		conn:      conn,
		recipient: recipient,
	})
}

func (writer *writer) Handle(msg interface{}) {
	switch msg := msg.(type) {
	case *Message:
		err := writeMessage(writer.conn, msg)
		if err != nil {
			writer.recipient.Send(MsgNetworkError{writer.hostId, err})
		} else {
			writer.recipient.Send(MsgMessageWritten{writer.hostId, msg})
		}
	case actor.MsgStop:
		writer.conn.Close()
	}
}

func writeMessage(to net.Conn, msg *Message) error {
	buf := bytes.NewBuffer(make([]byte, 4, 128))
	Encode(msg, buf)
	bufSize := buf.Len()
	PutUint32(buf.Bytes(), uint32(bufSize-4))
	n, err := to.Write(buf.Bytes())
	if err == nil && n != bufSize {
		return fmt.Errorf("Failed to write to %s.", to.RemoteAddr())
	}
	return err
}

func (writer *writer) logf(format string, params ...interface{}) {
	Log.Debugf(">>> writer-%s-%s: "+format, append([]interface{}{writer.conn.LocalAddr(), writer.conn.RemoteAddr()}, params...)...)
}
