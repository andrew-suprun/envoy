package dialer

import (
	"github.com/andrew-suprun/envoy/actor"
	. "github.com/andrew-suprun/envoy/messenger"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"net"
)

type dialer struct {
	actor.Actor
	recipient Sender
}

func NewDialer(recipient Sender) Sender {
	dialer := &dialer{
		Actor:     actor.NewActor("dialer"),
		recipient: recipient,
	}
	dialer.RegisterHandler("dial", dialer.handleDial).Start()
	return dialer
}

func (dialer *dialer) handleDial(_ string, info []interface{}) {
	addr := info[0].(HostId)

	conn, err := net.Dial("tcp", string(addr))
	if err != nil {
		dialer.recipient.Send("dial-error", addr, err)
	} else {
		dialer.recipient.Send("dialed", addr, conn)
	}

}

func (dialer *dialer) logf(format string, params ...interface{}) {
	Log.Debugf(">>> dialer: "+format, params...)
}
