package messenger

import (
	"bytes"
	"fmt"
	"github.com/andrew-suprun/envoy/actor"
	"log"
	"net"
	"runtime/debug"
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

	return srv.
		RegisterHandler("start", srv.handleStart).
		RegisterHandler("publish", srv.handlePublish).
		RegisterHandler("request", srv.handleRequest).
		RegisterHandler("read", srv.handleReply).
		RegisterHandler("stop", srv.handleStop).
		Start()
}

func (srv *server) handleStart(_ actor.MessageType, _ actor.Payload) {
	srv.logf("handleStart: entered\n")
	for !srv.stopped {
		srv.logf("handleStart: dialing\n")
		conn, err := net.Dial("tcp", string(srv.serverId))
		srv.logf("handleStart: dialed: err = %v\n", err)
		if err != nil {
			Log.Errorf("Failed to connect to '%s'. Will re-try.", srv.serverId)
			time.Sleep(RedialInterval)
			continue
		}

		err = writeJoinInvite(srv.hostId, conn)
		srv.logf("handleStart: sent invite: err = %v\n", err)
		if err != nil {
			Log.Errorf("Failed to invite '%s'. Will re-try.", srv.serverId)
			time.Sleep(RedialInterval)
			continue
		}

		reply, err := readJoinAccept(conn)
		srv.logf("handleStart: read acceptance: err = %v\n", err)
		if err != nil {
			Log.Errorf("Failed to read join accept from '%s'. Will re-try.", conn)
			time.Sleep(RedialInterval)
			continue
		}

		srv.reader = newReader(fmt.Sprintf("%s-%s-reader", srv.hostId, srv.serverId), conn, srv)
		srv.logf("handleStart: started reader\n")
		srv.writer = newWriter(fmt.Sprintf("%s-%s-writer", srv.hostId, srv.serverId), conn, srv)
		srv.logf("handleStart: started writer\n")

		srv.msgr.Send("join-accepted", &joinAcceptReply{srv.serverId, reply})
		srv.logf("sent joinAcceptReply = %#v\n", *reply)
		return
	}
}

func (srv *server) handleStop(_ actor.MessageType, _ actor.Payload) {
	srv.logf("--- entered handleStop ---")
	if srv.reader != nil {
		srv.reader.Stop()
		srv.logf("--- stopped reader ---")
	}

	if srv.writer != nil {
		srv.writer.Stop()
		srv.logf("--- stopped writer --- ")
	}
	srv.logf("--- exiting handleStop ---")
}

func (srv *server) handleMessage(_ actor.MessageType, info actor.Payload) {
	cmd := info.(*actorMessage)
	srv.logf("received message: %#v; stack:\n%s", cmd.message, debug.Stack())
}

func (srv *server) handlePublish(_ actor.MessageType, info actor.Payload) {
	srv.writer.Send("write", info)
}

func (srv *server) handleRequest(_ actor.MessageType, info actor.Payload) {
	cmd := info.(*requestCommand)
	srv.pendingReplies[cmd.MessageId] = cmd.replyChan
	srv.writer.Send("write", cmd.message)
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

func (srv *server) logf(format string, params ...interface{}) {
	log.Printf(">>> %s-%s-server: "+format, append([]interface{}{srv.hostId, srv.serverId}, params...)...)
}
