package server

import (
	"bytes"
	"fmt"
	"github.com/andrew-suprun/envoy/actor"
	"github.com/andrew-suprun/envoy/future"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"github.com/andrew-suprun/envoy/messenger/listener"
	"github.com/andrew-suprun/envoy/messenger/proxy"
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
	netListener   net.Listener
	client        actor.Actor
	leaveFuture   future.Future
}

type client struct {
	clientId   HostId
	serverAddr HostId
	reader     actor.Actor
	writer     actor.Actor
}

func NewServer(hostId HostId, _net proxy.Network) (actor.Actor, error) {
	s := &server{
		hostId:        hostId,
		subscriptions: make(map[Topic]Handler),
		clients:       make(map[HostId]*client),
	}
	s.self = actor.NewActor(s)

	var err error
	s.listener, s.netListener, err = listener.NewListener(hostId, s.self, _net)
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
		joinMsg := &JoinMessage{}
		for topic := range s.subscriptions {
			joinMsg.Topics = append(joinMsg.Topics, topic)
		}
		for _, client := range s.clients {
			if client.serverAddr != "" {
				joinMsg.Peers = append(joinMsg.Peers, client.serverAddr)
			}
		}

		clientId := HostId(msg.Conn.RemoteAddr().String())
		client := &client{
			clientId: clientId,
			reader:   reader.NewReader(clientId, msg.Conn, s.self),
			writer:   writer.NewWriter(clientId, msg.Conn, s.self),
		}

		s.clients[client.clientId] = client

		buf := &bytes.Buffer{}
		Encode(joinMsg, buf)

		client.writer.Send(&Message{
			Body:        buf.Bytes(),
			MessageId:   NewId(),
			MessageType: Join,
		})

		s.listener.Send(listener.MsgAccept{})

	case reader.MsgMessageRead:
		client := s.clients[msg.HostId]
		if client == nil {
			Log.Errorf("Received '%s' message from non-existing client %s. Ignored.", msg.Msg.MessageType, msg.HostId)
			return
		}

		switch msg.Msg.MessageType {
		case Join:
			s.handleJoin(client, msg.Msg)
		case Publish, Request:
			s.handleRequest(client, msg.Msg)
		default:
			panic(fmt.Sprintf("server cannot handle MsgMessageRead [%T]: %+v", msg, msg))
		}

	case writer.MsgMessageWritten:

	case MsgSubscribe:
		s.subscriptions[msg.Topic] = msg.Handler

	case MsgUnsubscribe:
		delete(s.subscriptions, msg.Topic)

	case MsgLeave:
		s.logf("~~~ MsgLeave")
		s.leaveFuture = msg.Result
		for _, client := range s.clients {
			client.writer.Send(&Message{MessageType: Leaving})
		}
		s.closeListener()
		if len(s.clients) == 0 {
			s.leaveFuture.SetValue(true)
		}

	case listener.MsgAcceptFailed:
		if s.leaveFuture != nil {
			return
		}
		// TODO
		s.logf("accept failed")
		panic("accept failed")

	case MsgNetworkError:
		s.logf("MsgNetworkError: host = %s; err = %v", msg.HostId, msg.Err)
		client := s.clients[msg.HostId]
		if client != nil {
			s.logf("MsgNetworkError: client = %s", client)
		} else {
			s.logf("MsgNetworkError: no client")
		}
		s.handleLeft(s.clients[msg.HostId])

	default:
		panic(fmt.Sprintf("server cannot handle message [%T]: %+v", msg, msg))
	}
}

func (s *server) closeListener() {
	if s.netListener == nil {
		return
	}
	s.netListener.Close()
	s.listener.Send(actor.MsgStop{})
	s.netListener = nil
	if s.leaveFuture != nil {
		s.leaveFuture.SetValue(false)
	}
}

func (s *server) forceShutdown() {
	for _, client := range s.clients {
		client.reader.Send(actor.MsgStop{})
		client.writer.Send(actor.MsgStop{})
	}
	if s.leaveFuture != nil {
		s.leaveFuture.SetValue(false)
	}
}

func (s *server) handleJoin(client *client, msg *Message) {
	var hostId HostId
	Decode(bytes.NewBuffer(msg.Body), &hostId)
	client.serverAddr = hostId
	s.client.Send(MsgAddHost{hostId})
	Log.Infof("Peer %s joined.", hostId)
}

func (s *server) handleLeft(client *client) {
	if client == nil {
		return
	}

	delete(s.clients, client.clientId)
	client.reader.Send(actor.MsgStop{})
	client.writer.Send(actor.MsgStop{})
	Log.Infof("Peer %s left.", client.serverAddr)

	if s.leaveFuture != nil && len(s.clients) == 0 {
		s.leaveFuture.SetValue(true)
	}
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

func (s *server) logf(format string, params ...interface{}) {
	Log.Debugf(">>> server-%s: "+format, append([]interface{}{s.hostId}, params...)...)
}
