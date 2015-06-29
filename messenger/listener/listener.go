package listener

import (
	"github.com/andrew-suprun/envoy/actor"
	. "github.com/andrew-suprun/envoy/messenger"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"log"
	"net"
)

type listener struct {
	actor.Actor
	hostId    HostId
	listener  net.Listener
	recipient Sender
	stopped   bool
}

func NewListener(hostId HostId, recipient Sender) (Sender, error) {
	lsnr := &listener{
		Actor:     actor.NewActor("listener-" + string(hostId)),
		hostId:    hostId,
		recipient: recipient,
	}
	lsnr.
		RegisterHandler("accept", lsnr.handleAccept).
		RegisterHandler("stop", lsnr.handleStop).
		Start()

	var err error
	lsnr.listener, err = net.Listen("tcp", string(hostId))
	if err != nil {
		Log.Errorf("Failed to listen on %s. Exiting.", hostId)
		return nil, err
	}
	Log.Infof("Listening on: %s", hostId)
	lsnr.Send("accept")
	return lsnr, nil
}

func (lsnr *listener) handleStop(_ string, _ []interface{}) {
	lsnr.stopped = true
	lsnr.listener.Close()
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

	lsnr.recipient.Send("accepted", conn)
}

func (lsnr *listener) String() string {
	return "listener: " + string(lsnr.hostId)
}

func (lsnr *listener) logf(format string, params ...interface{}) {
	log.Printf(">>> listener-"+string(lsnr.hostId)+": "+format, params...)
}
