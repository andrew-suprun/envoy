package messenger

import (
	"bytes"
	"fmt"
	"github.com/andrew-suprun/envoy/actor"
	"log"
	"net"
	"time"
)

type pendingReplies map[messageId]chan *actorMessage

type server struct {
	hostId
	actor.Actor
	serverId hostId
	pendingReplies
	reader  actor.Actor
	writer  actor.Actor
	msgr    actor.Actor
	stopped bool
}

func newServer(hostId, serverId hostId, msgr actor.Actor) actor.Actor {
	srv := &server{
		hostId:         hostId,
		serverId:       serverId,
		Actor:          actor.NewActor(fmt.Sprintf("%s-%s-server", hostId, serverId)),
		pendingReplies: make(pendingReplies),
		msgr:           msgr,
	}

	srv.
		RegisterHandler("start", srv.handleStart).
		RegisterHandler("publish", srv.handlePublish).
		RegisterHandler("request", srv.handleRequest).
		RegisterHandler("read", srv.handleRead).
		RegisterHandler("error", srv.handleError).
		RegisterHandler("stop", srv.handleStop)

	return srv
}

func (srv *server) handleStart(_ string, _ []interface{}) {
	conn, err := net.Dial("tcp", string(srv.serverId))
	if err != nil {
		Log.Errorf("Failed to connect to '%s'. Will re-try.", srv.serverId)
		srv.redial()
		return
	}

	err = writeJoinInvite(srv.hostId, conn)
	if err != nil {
		Log.Errorf("Failed to invite '%s'. Will re-try.", srv.serverId)
		srv.redial()
		return
	}

	reply, err := readJoinAccept(conn)
	if err != nil {
		Log.Errorf("Failed to read join accept from '%s'. Will re-try.", conn)
		srv.redial()
		return
	}

	// srv.reader = newReader(fmt.Sprintf("%s-%s-server-reader", srv.hostId, srv.serverId), conn, srv)
	// srv.writer = newWriter(fmt.Sprintf("%s-%s-server-writer", srv.hostId, srv.serverId), conn, srv)

	srv.msgr.Send("server-started", srv.serverId, reply)
	return
}

func (srv *server) redial() {
	time.AfterFunc(RedialInterval, func() {
		srv.Send("start")
	})
}

func (srv *server) handleStop(_ string, _ []interface{}) {
	for _, replyChan := range srv.pendingReplies {
		replyChan <- &actorMessage{error: ServerDisconnectedError}
	}

	if srv.reader != nil {
		srv.reader.Send("stop")
	}

	if srv.writer != nil {
		srv.writer.Send("stop")
	}
}

func (srv *server) handlePublish(_ string, info []interface{}) {
	srv.writer.Send("write", info...)
}

func (srv *server) handleRequest(_ string, info []interface{}) {
	msg := info[0].(*message)
	replyChan := info[1].(chan *actorMessage)
	srv.pendingReplies[msg.MessageId] = replyChan
	srv.writer.Send("write", msg)
}

func (srv *server) handleRead(_ string, info []interface{}) {
	msg := info[0].(*message)
	replyChan := srv.pendingReplies[msg.MessageId]
	if replyChan == nil {
		Log.Errorf("Received unexpected reply. Ignoring.")
	}
	delete(srv.pendingReplies, msg.MessageId)
	replyChan <- &actorMessage{message: msg}
}

func (srv *server) handleError(_ string, info []interface{}) {
	err := info[0].(error)
	Log.Errorf("%s-%s-server: Received network error: %v. Will try to re-connect.", srv.hostId, srv.serverId, err)
	srv.msgr.Send("server-error", srv.serverId, err)
}

func writeJoinInvite(hostId hostId, conn net.Conn) error {
	buf := &bytes.Buffer{}
	encode(hostId, buf)
	return writeMessage(conn, &message{MessageId: newId(), MessageType: join, Body: buf.Bytes()})
}

func readJoinAccept(conn net.Conn) (*joinMessage, error) {
	joinMsg, err := readMessage(conn)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(joinMsg.Body)
	reply := &joinMessage{}
	decode(buf, reply)
	return reply, nil
}

func (srv *server) logf(format string, params ...interface{}) {
	log.Printf(">>> %s-%s-server: "+format, append([]interface{}{srv.hostId, srv.serverId}, params...)...)
}
