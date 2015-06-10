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
	msgr    actor.Actor
	stopped bool
}

func newListener(name string, msgr actor.Actor, hostId hostId) (actor.Actor, error) {
	lsnr := &listener{
		name:   name,
		Actor:  actor.NewActor(name),
		hostId: hostId,
		msgr:   msgr,
	}
	lsnr.
		RegisterHandler("stop", lsnr.handleStop).
		Start()

	var err error
	lsnr.Listener, err = net.Listen("tcp", string(hostId))
	if err != nil {
		Log.Errorf("Failed to listen on %s. Exiting.", hostId)
		return nil, err
	}
	Log.Infof("Listening on: %s", lsnr.hostId)
	go lsnr.listen()
	return lsnr, nil
}

func (lsnr *listener) handleStop(_ actor.MessageType, _ actor.Payload) {
	if !lsnr.stopped {
		lsnr.stopped = true
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

		lsnr.logf("accepted %s", conn.RemoteAddr())
		clientId, err := lsnr.readJoinInvite(conn)
		lsnr.logf("read invite from %s; err = %v", clientId, err)
		if err != nil {
			Log.Errorf("Failed to read join invite: err = %v", err)
			continue
		}

		err = lsnr.writeJoinAccept(conn)
		lsnr.logf("sent acceptance to %s; err = %v", clientId, err)
		if err != nil {
			Log.Errorf("Failed to write join accept: err = %v", err)
			continue
		}

		lsnr.logf("sent new-client to msgr for client %s", clientId)
		lsnr.msgr.Send("new-client", &newPeerCommand{clientId, conn})
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

func (lsnr *listener) logf(format string, params ...interface{}) {
	log.Printf(">>> %s: "+format, append([]interface{}{lsnr.name}, params...)...)
}
