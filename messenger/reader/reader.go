package reader

import (
	"bytes"
	"github.com/andrew-suprun/envoy/actor"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"net"
)

type (
	MsgRead        struct{}
	MsgMessageRead struct {
		HostId HostId
		Msg    *Message
	}
)

type reader struct {
	hostId    HostId
	conn      net.Conn
	self      actor.Actor
	recipient actor.Actor
}

func NewReader(hostId HostId, conn net.Conn, recipient actor.Actor) actor.Actor {
	reader := &reader{
		hostId:    hostId,
		conn:      conn,
		recipient: recipient,
	}
	reader.self = actor.NewActor(reader)
	reader.self.Send(MsgRead{})
	return reader.self
}

func (r *reader) Handle(msg interface{}) {
	switch msg.(type) {
	case MsgRead:
		readMsg, err := readMessage(r.conn)
		if err != nil {
			r.recipient.Send(MsgNetworkError{r.hostId, err})
		} else {
			r.recipient.Send(MsgMessageRead{r.hostId, readMsg})
			r.self.Send(MsgRead{})
		}
	}
}

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
	Log.Debugf(">>> reader-%s-%s: "+format, append([]interface{}{reader.conn.LocalAddr(), reader.conn.RemoteAddr()}, params...)...)
}
