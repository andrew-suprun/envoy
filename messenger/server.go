package messenger

import (
	"bytes"
	"github.com/andrew-suprun/envoy/actor"
	"net"
	"time"
)

type pendingReplies map[messageId]chan *actorMessage

type server struct {
	actor.Actor
	hostId
	pendingReplies
	reader  actor.Actor
	writer  actor.Actor
	msgr    actor.Actor
	stopped bool
}

func newServer(serverId hostId, conn net.Conn, msgr actor.Actor) actor.Actor {
	srv := &server{
		hostId:         serverId,
		Actor:          actor.NewActor("server-" + string(serverId)),
		pendingReplies: make(pendingReplies),
		msgr:           msgr,
	}

	srv.reader = newReader("server-reader-"+string(serverId), conn, srv)
	srv.writer = newWriter("server-writer-"+string(serverId), conn, srv)

	return srv.
		RegisterHandler("publish", srv.handlePublish).
		RegisterHandler("request", srv.handleRequest).
		RegisterHandler("reply", srv.handleReply).
		Start()
}

func (srv *server) start() {
	for !srv.stopped {
		conn, err := net.Dial("tcp", string(srv.hostId))
		if err != nil {
			Log.Errorf("Failed to connect to '%s'. Will re-try.", srv.hostId)
			time.Sleep(RedialInterval)
			continue
		}

		err = writeJoinInvite(srv.hostId, conn)
		if err != nil {
			Log.Errorf("Failed to invite '%s'. Will re-try.", srv.hostId)
			time.Sleep(RedialInterval)
			continue
		}

		reply, err := readJoinAccept(conn)
		if err != nil {
			Log.Errorf("Failed to read join accept from '%s'. Will re-try.", conn)
			time.Sleep(RedialInterval)
			continue
		}

		srv.msgr.Send("got-peers", reply.Peers)
		return
	}
}

func (srv *server) handlePublish(_ actor.MessageType, info actor.Payload) {
	srv.writer.Send("message", info)
}

func (srv *server) handleRequest(_ actor.MessageType, info actor.Payload) {
	cmd := info.(*requestCommand)
	srv.pendingReplies[cmd.MessageId] = cmd.replyChan
	srv.writer.Send("message", cmd.message)
}

func (srv *server) handleReply(_ actor.MessageType, info actor.Payload) {
	msg := info.(*actorMessage)
	replyChan := srv.pendingReplies[msg.MessageId]
	if replyChan == nil {
		Log.Errorf("Received unexpected reply. Ignoring.")
	}
	replyChan <- msg
}

func (srv *server) stop() {
	for _, replyChan := range srv.pendingReplies {
		replyChan <- &actorMessage{error: ServerDisconnectedError}
	}
	srv.writer.Stop()
	srv.reader.Stop()
}

func writeJoinInvite(hostId hostId, conn net.Conn) error {
	buf := &bytes.Buffer{}
	encode(hostId, buf)
	return writeMessage(conn, &message{MessageId: newId(), MessageType: joinInvite, Body: buf.Bytes()})
}

func readJoinAccept(conn net.Conn) (*joinAcceptBody, error) {
	joinMsg, err := readMessage(conn)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(joinMsg.Body)
	reply := &joinAcceptBody{}
	decode(buf, reply)
	return reply, nil
}
