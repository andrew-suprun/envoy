package server

import (
	"bytes"
	"fmt"
	. "github.com/andrew-suprun/envoy"
	"github.com/andrew-suprun/envoy/actor"
	"github.com/andrew-suprun/envoy/future"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"github.com/andrew-suprun/envoy/messenger/listener"
	"github.com/andrew-suprun/envoy/messenger/reader"
	"github.com/andrew-suprun/envoy/messenger/writer"
	"net"
	"runtime/debug"
)

type (
	MsgClient    struct{ Client actor.Actor }
	MsgSubscribe struct {
		Topic   Topic
		Handler Handler
	}
	MsgUnsubscribe struct{ Topic Topic }
)

type server struct {
	self          actor.Actor
	hostId        HostId
	subscriptions map[Topic]Handler
	clients       map[HostId]*client
	listener      actor.Actor
	client        actor.Actor
	leaveFuture   future.Future
}

type client struct {
	clientId       HostId
	topics         map[Topic]struct{}
	pendingReplies map[MsgId]future.Future
	reader         actor.Actor
	writer         actor.Actor
	conn           net.Conn
	state          state
}

type state int

const (
	accepted state = iota
)

func NewServer(hostId HostId) (actor.Actor, error) {
	s := &server{
		hostId:        hostId,
		subscriptions: make(map[Topic]Handler),
		clients:       make(map[HostId]*client),
	}
	s.self = actor.NewActor(s)

	var err error
	s.listener, err = listener.NewListener(hostId, s.self)
	if err != nil {
		return nil, err
	}

	return s.self, nil
}

func (s *server) Handle(msg interface{}) {
	switch msg := msg.(type) {
	case MsgClient:
		s.client = msg.Client
	case listener.MsgConnAccepted:
		s.logf("handleAccepted: msg = %#v", msg)
		client := &client{
			clientId:       HostId(msg.Conn.RemoteAddr().String()),
			topics:         make(map[Topic]struct{}),
			pendingReplies: make(map[MsgId]future.Future),
			conn:           msg.Conn,
			state:          accepted,
			reader:         nil,
		}
		client.reader = reader.NewReader(client.clientId, msg.Conn, s.self)
		client.writer = writer.NewWriter(client.clientId, msg.Conn, s.self)

		s.clients[client.clientId] = client

		joinMsg := &JoinMessage{HostId: s.hostId}
		for topic := range s.subscriptions {
			joinMsg.Topics = append(joinMsg.Topics, topic)
		}
		for _, peer := range s.clients {
			joinMsg.Peers = append(joinMsg.Peers, peer.clientId)
		}

		buf := &bytes.Buffer{}
		Encode(joinMsg, buf)

		client.writer.Send(&Message{
			Body:        buf.Bytes(),
			MessageId:   NewId(),
			MessageType: Join,
		})
		s.listener.Send(nil)
	case listener.MsgAcceptFailed:
	case reader.MsgMessageRead:
		client := s.clients[msg.HostId]
		if client == nil {
			Log.Errorf("Received '%s' message from non-existing client %s. Ignored.", msg.Msg.MessageType, msg.HostId)
			s.logf("Clients %d.", len(s.clients))
			for clId := range s.clients {
				s.logf("Client %s.", clId)
			}
			return
		}

		switch msg.Msg.MessageType {
		case Join:
			s.handleJoin(client, msg.Msg)
		case Publish, Request:
			s.handleRequest(client, msg.Msg)
		case Leaving:
			s.handleLeaving(client, msg.Msg)
		default:
			panic(fmt.Sprintf("received message: %v", msg.Msg))
		}
	case MsgNetworkError:
	default:
	}
}

func (s *server) handleJoin(client *client, msg *Message) {
	var joinMsg JoinMessage
	Decode(bytes.NewBuffer(msg.Body), &joinMsg)
	s.logf("handleJoin: joinMsg = %v", joinMsg)
	s.client.Send(MsgAddHost{joinMsg.HostId})
}

func (s *server) handleRequest(client *client, msg *Message) {
	handler := s.subscriptions[msg.Topic]
	if handler == nil {
		Log.Errorf("Received '%s' message for non-subscribed topic %s. Ignored.", msg.MessageType, msg.Topic)
		return
	}

	go s.runHandler(client, msg, handler)
}

func (s *server) runHandler(client *client, msg *Message, handler Handler) {
	result, err := s.runHandlerProtected(msg, handler)
	if msg.MessageType == Publish {
		return
	}

	reply := &Message{
		MessageId:   msg.MessageId,
		MessageType: Reply,
		Body:        result,
	}

	if err == PanicError {
		reply.MessageType = ReplyPanic
	}

	client.writer.Send(reply)
}

func (s *server) runHandlerProtected(msg *Message, handler Handler) (result []byte, err error) {
	defer func() {
		recErr := recover()
		if recErr != nil {
			Log.Panic(recErr, string(debug.Stack()))
			result = nil
			err = PanicError
		}
	}()

	result = handler(string(msg.Topic), msg.Body, msg.MessageId)
	return result, err

}

func (s *server) handleNetworkError(_ string, info []interface{}) {
	panic("Implement me.")
	// clientId := info[0].(HostId)
	// err := info[1].(error)
	// if s.leaveFuture != nil {
	// 	s.Send("shutdown-client", clientId)
	// 	return
	// }
	// if client, found := s.clients[clientId]; found {
	// 	if client.state == clientStopping || client.state == clientLeaving {
	// 		return
	// 	}
	// 	client.state = clientStopping
	// 	if err.Error() == "EOF" {
	// 		Log.Errorf("Peer %s disconnected. Will try to re-connect.", clientId)
	// 	} else {
	// 		Log.Errorf("Peer %s: Network error: %v. Will try to re-connect.", clientId, err)
	// 	}

	// 	s.Send("shutdown-client", client.clientId)

	// 	if clientId > s.hostId {
	// 		time.AfterFunc(time.Millisecond, func() {
	// 			s.Send("dial", clientId)
	// 		})
	// 	} else {
	// 		time.AfterFunc(RedialInterval, func() {
	// 			s.Send("dial", clientId)
	// 		})
	// 	}
	// }
}

func (s *server) handleLeaving(client *client, msg *Message) {
	panic("Implement me.")
	// client.state = clientLeaving

	// pendingFutures := make([]future.Future, len(client.pendingReplies))
	// for _, pf := range client.pendingReplies {
	// 	pendingFutures = append(pendingFutures, pf)
	// }
	// go stopPeer(client, pendingFutures, msgr)
}

func (s *server) handleLeave(_ string, info []interface{}) {
	// s.broadcastMessage("", nil, Leaving)
	// s.listener.Stop()
	// s.self.Send("shutdown")
	// time.AfterFunc(timeout, func() { s.leaveFuture.SetValue(false) })
	// s.leaveFuture.Value()
	s.self.Send("stop")
}

func (s *server) handleSubscribe(_ string, info []interface{}) {
	// panic("Implement me.")
	// msg := info[0].(*Message)
	// replies := info[1].(future.Future)
	// responses := make([]future.Future, 0, len(s.clients))

	// for _, client := range s.clients {
	// 	response := future.NewFuture()
	// 	responses = append(responses, response)
	// 	client.pendingReplies[msg.MessageId] = response
	// 	client.writer.Send("write", msg)
	// }
	// replies.SetValue(responses)
}

func (s *server) logf(format string, params ...interface{}) {
	Log.Debugf(">>> server-%s: "+format, append([]interface{}{s.hostId}, params...)...)
}
