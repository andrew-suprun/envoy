package reader

import (
	"bytes"
	"github.com/andrew-suprun/envoy/actor"
	. "github.com/andrew-suprun/envoy/messenger"
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
		Start().
		Send("read-message")
	return reader
}

func (reader *reader) handleReadMessage(_ string, _ []interface{}) {
	msg, err := readMessage(reader.conn)
	if err != nil {
		reader.recipient.Send("network-error", reader.hostId, err)
	} else {
		reader.recipient.Send("message", reader.hostId, msg)
		reader.Send("read-message")
	}
}

// todo: add timeout handling
func readMessage(from net.Conn) (*Message, error) {
	if from == nil {
		return nil, NilConnError
	}
	lenBuf := make([]byte, 4)
	readBuf := lenBuf

	for len(readBuf) > 0 {
		n, err := from.Read(readBuf)
		if err != nil {
			return nil, err
		}
		readBuf = readBuf[n:]
	}

	msgSize := GetUint32(lenBuf)
	msgBytes := make([]byte, msgSize)
	readBuf = msgBytes
	for len(readBuf) > 0 {
		n, err := from.Read(readBuf)
		if err != nil {
			return nil, err
		}
		readBuf = readBuf[n:]
	}
	msgBuf := bytes.NewBuffer(msgBytes)
	msg := &Message{}
	Decode(msgBuf, msg)
	return msg, nil
}

func (reader *reader) logf(format string, params ...interface{}) {
	Log.Debugf(">>> %s: "+format, append([]interface{}{reader.name}, params...)...)
}
