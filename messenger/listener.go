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
	hostId
	net.Listener
	msgr actor.Actor
}

func newListener(name string, msgr actor.Actor, hostId hostId) (actor.Actor, error) {
	lsnr := &listener{
		name:   name,
		Actor:  actor.NewActor(name),
		hostId: hostId,
		msgr:   msgr,
	}
	lsnr.
		RegisterHandler("accept", lsnr.handleAccept).
		RegisterHandler("stop", lsnr.handleStop)

	var err error
	lsnr.Listener, err = net.Listen("tcp", string(hostId))
	if err != nil {
		Log.Errorf("Failed to listen on %s. Exiting.", hostId)
		return nil, err
	}
	Log.Infof("Listening on: %s", lsnr.hostId)
	lsnr.Send("accept")
	return lsnr, nil
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

	clientId, err := lsnr.readJoinInvite(conn)
	if err != nil {
		Log.Errorf("Failed to read join invite: err = %v", err)
		return
	}

	err = lsnr.writeJoinAccept(conn)
	if err != nil {
		Log.Errorf("Failed to write join accept: err = %v", err)
		return
	}

	lsnr.msgr.Send("new-client", clientId, conn)
}

func (lsnr *listener) readJoinInvite(conn net.Conn) (hostId, error) {
	joinMsg, err := readMessage(conn)
	if err != nil {
		return "", err
	}

	buf := bytes.NewBuffer(joinMsg.Body)
	var reply hostId
	decode(buf, &reply)
	return reply, nil
}

func (lsnr *listener) writeJoinAccept(conn net.Conn) error {
	responseChan := make(chan *joinAcceptBody)
	lsnr.msgr.Send("join-accept", responseChan)
	buf := &bytes.Buffer{}
	encode(<-responseChan, buf)
	return writeMessage(conn, &message{MessageId: newId(), MessageType: joinAccept, Body: buf.Bytes()})
}

func (lsnr *listener) logf(format string, params ...interface{}) {
	log.Printf(">>> %s: "+format, append([]interface{}{lsnr.name}, params...)...)
}
