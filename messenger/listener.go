package messenger

import (
	"bytes"
	"github.com/andrew-suprun/envoy/actor"
	"net"
)

type listener struct {
	actor.Actor
	hostId
	net.Listener
	msgr    actor.Actor
	stopped bool
}

func newListener(msgr *messenger, hostId hostId) (actor.Actor, error) {
	lsnr := &listener{
		Actor:  actor.NewActor("listener"),
		hostId: hostId,
		msgr:   msgr,
	}
	lsnr.
		RegisterHandler("start", lsnr.handleStart).
		RegisterHandler("stop", lsnr.handleStop).
		Start()

	var err error
	lsnr.Listener, err = net.Listen("tcp", string(hostId))
	if err != nil {
		Log.Errorf("Failed to listen on %s. Exiting.", hostId)
		return nil, err
	}
	Log.Infof("Listening on: %s", lsnr.hostId)
	return lsnr, nil
}

func (lsnr *listener) handleStart(_ actor.MessageType, _ actor.Payload) {
	lsnr.listen()
}

func (lsnr *listener) handleStop(_ actor.MessageType, _ actor.Payload) {
	if !lsnr.stopped {
		lsnr.stopped = true
		lsnr.Stop()
		lsnr.Listener.Close()
	}
}

func (lsnr *listener) listen() {
	for !lsnr.stopped {
		conn, err := lsnr.Listener.Accept()
		if err != nil {
			Log.Errorf("Failed to accept connection: err = %v", err)
			continue
		}

		clientId, err := lsnr.readJoinInvite(conn)
		if err != nil {
			Log.Errorf("Failed to read join invite: err = %v", err)
			continue
		}

		err = lsnr.writeJoinAccept(conn)
		if err != nil {
			Log.Errorf("Failed to write join accept: err = %v", err)
			continue
		}

		lsnr.msgr.Send("new-client", newPeerCommand{clientId, conn})
	}
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
	lsnr.msgr.Send("join-accept", joinAcceptCommand{responseChan})
	buf := &bytes.Buffer{}
	encode(<-responseChan, buf)
	return writeMessage(conn, &message{MessageId: newId(), MessageType: joinAccept, Body: buf.Bytes()})
}
