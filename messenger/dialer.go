package messenger

import (
	"bytes"
	"github.com/andrew-suprun/envoy/actor"
	"github.com/andrew-suprun/envoy/future"
	"net"
	"time"
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
	dialer.RegisterHandler("dial", dialer.handleDial)
	return dialer
}

func (dialer *dialer) handleDial(_ string, info []interface{}) {
	addr := info[0].(hostId)
	joinMsg := info[1].(*joinMessage)
	var result future.Future
	if len(info) > 2 && info[2] != nil {
		result = info[2].(future.Future)
	}

	dialer.logf("handleDial: addr = %s; joinMsg = %v; result = %s", addr, joinMsg, result)
	defer func() {
		dialer.logf("handleDial: done")
	}()

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
		Log.Errorf("Failed to connect to '%s'. Will re-try.", addr)
		dialer.redial(addr, joinMsg, result)
		return
	}

	err = writeMessage(conn, msg)
	if err != nil {
		Log.Errorf("Failed to invite '%s'. Will re-try.", addr)
		dialer.redial(addr, joinMsg, result)
		return
	}

	replyMsg, err := readMessage(conn)
	if err != nil {
		Log.Errorf("Failed to read join accept from '%s'. Will re-try.", conn)
		dialer.redial(addr, joinMsg, result)
		return
	}

	buf = bytes.NewBuffer(replyMsg.Body)
	reply := &joinMessage{}
	decode(buf, reply)

	dialer.msgr.Send("dialed", addr, conn, reply, result)

	return
}

func (dialer *dialer) redial(addr hostId, joinMsg *joinMessage, result future.Future) {
	time.AfterFunc(RedialInterval, func() {
		dialer.Send("dial", addr, joinMsg, result)
	})
}

func (dialer *dialer) logf(format string, params ...interface{}) {
	Log.Debugf(">>> %s: "+format, append([]interface{}{dialer.name}, params...)...)
}
