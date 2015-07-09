package listener

import (
	"fmt"
	"github.com/andrew-suprun/envoy/actor"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"github.com/andrew-suprun/envoy/messenger/proxy"
	"log"
	"net"
)

type (
	MsgAccept       struct{}
	MsgConnAccepted struct{ net.Conn }
	MsgAcceptFailed struct{}
)

type listener struct {
	self      actor.Actor
	hostId    HostId
	listener  net.Listener
	recipient actor.Actor
	stopped   bool
}

func NewListener(hostId HostId, recipient actor.Actor, _net proxy.Network) (actor.Actor, net.Listener, error) {
	lsnr := &listener{
		hostId:    hostId,
		recipient: recipient,
	}
	lsnr.self = actor.NewActor(lsnr)

	var err error
	lsnr.listener, err = _net.Listen(hostId)
	if err != nil {
		Log.Errorf("Failed to listen on %s. Exiting.", hostId)
		return nil, nil, err
	}
	Log.Infof("Listening on: %s", hostId)
	lsnr.self.Send(MsgAccept{})
	return lsnr.self, lsnr.listener, nil
}

func (lsnr *listener) Handle(msg interface{}) {
	switch msg.(type) {
	case MsgAccept:
		conn, err := lsnr.listener.Accept()
		if err != nil {
			lsnr.recipient.Send(MsgAcceptFailed{})
			return
		}
		lsnr.recipient.Send(MsgConnAccepted{conn})
	case actor.MsgStop:
		// nothing to do
	default:
		panic(fmt.Sprintf("listener got unknown message[%T]: %v", msg, msg))
	}
}

func (lsnr *listener) String() string {
	return "listener: " + string(lsnr.hostId)
}

func (lsnr *listener) logf(format string, params ...interface{}) {
	log.Printf(">>> listener-"+string(lsnr.hostId)+": "+format, params...)
}
