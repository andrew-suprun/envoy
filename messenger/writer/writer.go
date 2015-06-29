package writer

import (
	"bytes"
	"fmt"
	"github.com/andrew-suprun/envoy/actor"
	. "github.com/andrew-suprun/envoy/messenger"
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
		Start()
}

func (writer *writer) handleWrite(_ string, info []interface{}) {
	msg := info[0].(*Message)
	err := writeMessage(writer.conn, msg)
	if err != nil {
		writer.recipient.Send("write-error", writer.hostId, err)
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
	Log.Debugf(">>> %s: "+format, append([]interface{}{writer.name}, params...)...)
}
