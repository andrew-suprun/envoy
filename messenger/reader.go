package messenger

import (
	"github.com/andrew-suprun/envoy/actor"
	"net"
)

type reader struct {
	actor.Actor
	net.Conn
	recipient actor.Actor
	stopped   bool
}

func newReader(name string, conn net.Conn, recipient actor.Actor) actor.Actor {
	reader := &reader{
		Actor:     actor.NewActor(name),
		Conn:      conn,
		recipient: recipient,
	}

	reader.RegisterHandler("stop", reader.handleStop)

	go reader.run()

	return reader.Start()
}

func (rdr *reader) run() {
	for !rdr.stopped {
		msg, err := readMessage(rdr.Conn)
		rdr.recipient.Send("read", &actorMessage{msg, err})

		if err != nil {
			rdr.Stop()
			return
		}
	}
}

func (rdr *reader) handleStop(_ actor.MessageType, _ actor.Payload) {
	if !rdr.stopped {
		rdr.stopped = true
		rdr.Close()
	}
}
