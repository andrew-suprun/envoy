package messenger

import (
	"bytes"
	"github.com/andrew-suprun/envoy/actor"
	"log"
	"net"
)

type listener struct {
	name string
	actor.Actor
	net.Listener
	joinMsg *joinMessage
	msgr    actor.Actor
}

func newListener(name string, msgr actor.Actor, joinMsg *joinMessage) (actor.Actor, error) {
	lsnr := &listener{
		name:    name,
		Actor:   actor.NewActor(name),
		joinMsg: joinMsg,
		msgr:    msgr,
	}
	lsnr.
		RegisterHandler("set-join-message", lsnr.handleSetJoinMessage).
		RegisterHandler("accept", lsnr.handleAccept).
		RegisterHandler("stop", lsnr.handleStop)

	var err error
	lsnr.Listener, err = net.Listen("tcp", string(lsnr.joinMsg.HostId))
	if err != nil {
		Log.Errorf("Failed to listen on %s. Exiting.", lsnr.joinMsg.HostId)
		return nil, err
	}
	Log.Infof("Listening on: %s", lsnr.joinMsg.HostId)
	lsnr.Send("accept")
	return lsnr, nil
}

func (lsnr *listener) handleSetJoinMessage(_ string, info []interface{}) {
	lsnr.joinMsg = info[0].(*joinMessage)
}

func (lsnr *listener) handleStop(_ string, _ []interface{}) {
	lsnr.Listener.Close()
}

func (lsnr *listener) handleAccept(_ string, _ []interface{}) {
	defer lsnr.Send("accept")

	conn, err := lsnr.Listener.Accept()
	if err != nil {
		Log.Errorf("Failed to accept connection: err = %v", err)
		return
	}

	joinMsg, err := lsnr.readJoinInvite(conn)
	if err != nil {
		Log.Errorf("Failed to read join invite: err = %v", err)
		return
	}

	lsnr.msgr.Send("accepted", hostId(conn.RemoteAddr().String()), conn, joinMsg)
}

func (lsnr *listener) readJoinInvite(conn net.Conn) (*joinMessage, error) {
	msg, err := readMessage(conn)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(msg.Body)
	var reply joinMessage
	decode(buf, &reply)
	return &reply, nil
}

func (lsnr *listener) logf(format string, params ...interface{}) {
	log.Printf(">>> %s: "+format, append([]interface{}{lsnr.name}, params...)...)
}
