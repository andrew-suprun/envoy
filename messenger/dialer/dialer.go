package dialer

import (
	"bytes"
	"github.com/andrew-suprun/envoy/actor"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"log"
	"net"
)

type dialer struct {
	actor.Actor
	hostId    HostId
	recipient Sender
}

func NewDialer(hostId HostId, recipient Sender) Sender {
	dialer := &dialer{
		hostId:    hostId,
		Actor:     actor.NewActor("dialer"),
		recipient: recipient,
	}
	dialer.
		RegisterHandler("dial", dialer.handleDial).
		RegisterHandler("leave", dialer.handleLeave).
		Start()
	return dialer
}

func (dialer *dialer) handleDial(_ string, info []interface{}) {
	addr := info[0].(HostId)
	joinMsg := info[1].(*JoinMessage)

	conn, err := net.Dial("tcp", string(addr))
	if err != nil {
		dialer.recipient.Send("dial-error", addr, err)
		return
	}

	if dialer.hostId < addr {
		buf := &bytes.Buffer{}
		Encode(dialer.hostId, buf)
		reqDial := &Message{
			MessageType: RequestDial,
			Body:        buf.Bytes(),
		}
		err = WriteMessage(conn, reqDial)
		conn.Close()
		if err != nil {
			dialer.recipient.Send("dial-error", addr, err)
		}
		return
	}

	buf := &bytes.Buffer{}
	Encode(joinMsg, buf)
	msg := &Message{
		MessageType: Join,
		Body:        buf.Bytes(),
	}
	err = WriteMessage(conn, msg)
	if err != nil {
		conn.Close()
		dialer.recipient.Send("dial-error", addr, err)
		return
	}

	replyMsg, err := ReadMessage(conn)
	if err != nil {
		conn.Close()
		dialer.recipient.Send("dial-error", addr, err)
		return
	}

	buf = bytes.NewBuffer(replyMsg.Body)
	reply := &JoinMessage{}
	Decode(buf, reply)

	dialer.recipient.Send("dialed", conn, reply)
}

func (dialer *dialer) handleLeave(_ string, _ []interface{}) {
	// TODO: ?
	dialer.Stop()
}

func (dialer *dialer) logf(format string, params ...interface{}) {
	log.Printf(">>> dialer-"+string(dialer.hostId)+": "+format, params...)
}
