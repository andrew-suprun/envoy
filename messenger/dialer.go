package messenger

import (
	"bytes"
	"github.com/andrew-suprun/envoy/actor"
	"github.com/andrew-suprun/envoy/future"
	"net"
)

type dialer struct {
	name string
	actor.Actor
	msgr actor.Actor
}

func newDialer(name string, msgr actor.Actor) actor.Actor {
	dialer := &dialer{
		name:  name,
		Actor: actor.NewActor(name),
		msgr:  msgr,
	}
	dialer.RegisterHandler("dial", dialer.handleDial).Start()
	return dialer
}

func (dialer *dialer) handleDial(_ string, info []interface{}) {
	addr := info[0].(hostId)
	joinMsg := info[1].(*joinMessage)
	var result future.Future
	if len(info) > 2 && info[2] != nil {
		result = info[2].(future.Future)
	}

	buf := &bytes.Buffer{}
	encode(joinMsg, buf)
	msg := &message{
		MessageId:   newId(),
		MessageType: join,
		Body:        buf.Bytes(),
	}

	if result != nil {
		defer result.SetValue(true)
	}

	conn, err := net.Dial("tcp", string(addr))
	if err != nil {
		dialer.reportDialError(addr, result, err)
		return
	}

	err = writeMessage(conn, msg)
	if err != nil {
		dialer.reportDialError(addr, result, err)
		return
	}

	replyMsg, err := readMessage(conn)
	if err != nil {
		dialer.reportDialError(addr, result, err)
		return
	}

	buf = bytes.NewBuffer(replyMsg.Body)
	reply := &joinMessage{}
	decode(buf, reply)

	dialer.msgr.Send("dialed", conn, reply, result)

	return
}
func (dialer *dialer) reportDialError(peerId hostId, result future.Future, err error) {
	if result != nil {
		result.SetError(err)
	}
	dialer.msgr.Send("dial-error", peerId)
}

func (dialer *dialer) logf(format string, params ...interface{}) {
	Log.Debugf(">>> %s: "+format, append([]interface{}{dialer.name}, params...)...)
}
