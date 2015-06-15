package messenger

import (
	"bytes"
	"github.com/andrew-suprun/envoy/actor"
	"net"
	"sync"
	"time"
)

type dialer struct {
	actor.Actor
	msgr actor.Actor
}

func newDialer(name string, msgr actor.Actor) actor.Actor {
	dialer := &dialer{
		Actor: actor.NewActor(name),
		msgr:  msgr,
	}
	dialer.RegisterHandler("dial", dialer.handleDial)
	return dialer
}

func (dialer *dialer) handleDial(_ string, info []interface{}) {
	addr := info[0].(hostId)
	joinMsg := info[1].(*message)
	var wg *sync.WaitGroup
	if len(info) >= 2 {
		wg = info[1].(*sync.WaitGroup)
	}

	if wg != nil {
		defer wg.Done()
	}

	conn, err := net.Dial("tcp", string(addr))
	if err != nil {
		Log.Errorf("Failed to connect to '%s'. Will re-try.", addr)
		dialer.redial(addr, joinMsg)
		return
	}

	err = writeMessage(conn, joinMsg)
	if err != nil {
		Log.Errorf("Failed to invite '%s'. Will re-try.", addr)
		dialer.redial(addr, joinMsg)
		return
	}

	replyMsg, err := readMessage(conn)
	if err != nil {
		Log.Errorf("Failed to read join accept from '%s'. Will re-try.", conn)
		dialer.redial(addr, joinMsg)
		return
	}

	buf := bytes.NewBuffer(replyMsg.Body)
	reply := &joinMessage{}
	decode(buf, reply)

	dialer.msgr.Send("connected", addr, conn, reply)

	return
}

func (dialer *dialer) redial(addr hostId, joinMsg *message) {
	time.AfterFunc(RedialInterval, func() {
		dialer.Send("dial", addr, joinMsg)
	})
}
