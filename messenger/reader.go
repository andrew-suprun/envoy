package messenger

import (
	"github.com/andrew-suprun/envoy/actor"
	"net"
)

type reader struct {
	actor.Actor
	conn      *net.Conn
	recipient actor.Actor
	stopped   bool
}

func newReader(conn net.Conn, recipient actor.Actor) actor.Actor {
	reader := &reader{
		reader:    actor.NewActor("reader"),
		conn:      conn,
		recipient: recipient,
	}

	reader.run()

	return reader.
		RegisterHandler("stop", reader.handleStop).
		Start()
}

func (rdr *reader) handleStop(_ actor.MessageType, _ actor.Payload) {
	if !rdr.stopped {
		rdr.stopped = true
		rdr.Stop()
		rdr.conn.Close()
	}
}

func (rdr *reader) run() {
	for !rdr.stopped {
		msg, err := readMessage(rdr.conn)

		rdr.recipientSend("message", &actorMessage{msg, err})

		if err != nil {
			rdr.stop()
		}
	}
}
