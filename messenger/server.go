package messenger

import (
	"bytes"
	"github.com/andrew-suprun/envoy/actor"
	"log"
	"net"
	"runtime/debug"
	"time"
)

type pendingReplies map[messageId]chan *actorMessage

type server struct {
	name string
	actor.Actor
	hostId
	pendingReplies
	reader  actor.Actor
	writer  actor.Actor
	msgr    actor.Actor
	stopped bool
}

func newServer(name string, serverId hostId, msgr actor.Actor) actor.Actor {
	srv := &server{
		name:           name,
		hostId:         serverId,
		Actor:          actor.NewActor(name),
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
		conn, err := net.Dial("tcp", string(srv.hostId))
		srv.logf("handleStart: dialed: err = %v\n", err)
		if err != nil {
			Log.Errorf("Failed to connect to '%s'. Will re-try.", srv.name)
			time.Sleep(RedialInterval)
			continue
		}

		err = writeJoinInvite(srv.hostId, conn)
		srv.logf("handleStart: sent invite: err = %v\n", err)
		if err != nil {
			Log.Errorf("Failed to invite '%s'. Will re-try.", srv.hostId)
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

		srv.reader = newReader(srv.name+"-reader", conn, srv)
		srv.logf("handleStart: started reader\n")
		srv.writer = newWriter(srv.name+"-writer", conn, srv)
		srv.logf("handleStart: started writer\n")

		srv.logf("sending joinAcceptReply = %+v\n", &joinAcceptReply{srv.hostId, reply})
		srv.msgr.Send("join-accepted", &joinAcceptReply{srv.hostId, reply})
		srv.logf("sent joinAcceptReply = %+v\n", &joinAcceptReply{srv.hostId, reply})
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
	log.Printf(">>> %s: "+format, append([]interface{}{srv.name}, params...)...)
}
