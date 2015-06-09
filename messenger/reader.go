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

	reader.run()

	return reader.Start()
}

func (rdr *reader) run() {
	for !rdr.stopped {
		msg, err := readMessage(rdr.Conn)
		rdr.recipient.Send("message", &actorMessage{msg, err})

		if err != nil {
			rdr.stop()
			return
		}
	}
}

func (rdr *reader) stop() {
	if !rdr.stopped {
		rdr.stopped = true
		rdr.Stop()
		rdr.Close()
	}
	// todo: graceful shutdown
}
