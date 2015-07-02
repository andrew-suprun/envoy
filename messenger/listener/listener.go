package listener

import (
	"bytes"
	. "github.com/andrew-suprun/envoy"
	"github.com/andrew-suprun/envoy/actor"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"log"
	"net"
)

type listener struct {
	actor.Actor
	hostId    HostId
	listener  net.Listener
	joinMsg   *JoinMessage
	recipient Sender
	stopped   bool
}

func NewListener(hostId HostId, recipient Sender, joinMsg *JoinMessage) (Sender, error) {
	lsnr := &listener{
		Actor:     actor.NewActor("listener-" + string(hostId)),
		hostId:    hostId,
		joinMsg:   joinMsg,
		recipient: recipient,
	}
	lsnr.
		RegisterHandler("accept", lsnr.handleAccept).
		RegisterHandler("leave", lsnr.handleLeave).
		Start()

	var err error
	lsnr.listener, err = net.Listen("tcp", string(hostId))
	if err != nil {
		Log.Errorf("Failed to listen on %s: %v", hostId, err)
		return nil, err
	}
	Log.Infof("Listening on: %s", hostId)
	lsnr.recipient.Send("listening", lsnr.listener)
	lsnr.Send("accept")
	return lsnr, nil
}

func (lsnr *listener) handleLeave(_ string, _ []interface{}) {
	// TODO: ?
	lsnr.logf("leaving")
	lsnr.stopped = true
	lsnr.listener.Close()
	lsnr.Stop()
}

func (lsnr *listener) handleAccept(_ string, _ []interface{}) {
	defer func() {
		if !lsnr.stopped {
			lsnr.Send("accept")
		}
	}()

	conn, err := lsnr.listener.Accept()
	if err != nil {
		if !lsnr.stopped {
			Log.Errorf("Failed to accept connection: err = %v", err)
		}
		return
	}

	msg, err := ReadMessage(conn)
	if err != nil {
		Log.Errorf("Failed to read join invite: err = %v", err)
		return
	}

	buf := bytes.NewBuffer(msg.Body)

	switch msg.MessageType {
	case Join:
		var joinMsg JoinMessage
		Decode(buf, &joinMsg)
		lsnr.recipient.Send("accepted", conn, &joinMsg)
	case RequestDial:
		var remote HostId
		Decode(buf, &remote)
		lsnr.recipient.Send("dial", remote)
		conn.Close()
	}
}

func (lsnr *listener) String() string {
	return "listener: " + string(lsnr.hostId)
}

func (lsnr *listener) logf(format string, params ...interface{}) {
	log.Printf(">>> listener-"+string(lsnr.hostId)+": "+format, params...)
}
