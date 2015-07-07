package client

import (
	"bytes"
	"fmt"
	. "github.com/andrew-suprun/envoy"
	"github.com/andrew-suprun/envoy/actor"
	"github.com/andrew-suprun/envoy/future"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"github.com/andrew-suprun/envoy/messenger/proxy"
	"github.com/andrew-suprun/envoy/messenger/reader"
	"github.com/andrew-suprun/envoy/messenger/writer"
	"math/rand"
	"net"
	"time"
)

type client struct {
	self        actor.Actor
	clientId    HostId
	proxy       proxy.Network
	servers     map[HostId]*server
	dialResult  future.Future
	leaveFuture future.Future
}

type server struct {
	serverId       HostId
	topics         map[Topic]struct{}
	pendingReplies map[MsgId]future.Future
	reader         actor.Actor
	writer         actor.Actor
	conn           net.Conn
	state          state
}

type state int

const (
	pendingDial state = iota
	pendingRedial
	dialing
	dialed
	connected
)

type (
	MsgJoin struct {
		HostIds []HostId
		Result  future.Future
	}
	MsgPublish struct {
		Topic   Topic
		Body    []byte
		MsgType MsgType
		Result  future.Future
	}
)

type (
	msgDialer    struct{}
	msgDial      struct{}
	msgDialError struct {
		serverId HostId
		err      error
	}
	msgDialed struct {
		serverId HostId
		conn     net.Conn
	}
)

func NewClent(clientId HostId, _net proxy.Network) actor.Actor {
	c := &client{
		clientId: clientId,
		proxy:    _net,
		servers:  make(map[HostId]*server),
	}
	c.self = actor.NewActor(c)
	return c.self
}

func newServer(serverId HostId) *server {
	return &server{
		serverId:       serverId,
		topics:         make(map[Topic]struct{}),
		pendingReplies: make(map[MsgId]future.Future),
		state:          pendingDial,
	}
}

func (c *client) Handle(msg interface{}) {
	if c.leaveFuture != nil {
		return
	}
	switch msg := msg.(type) {
	case MsgJoin:
		if len(msg.HostIds) == 0 {
			msg.Result.SetValue(true)
			return
		}
		c.dialResult = msg.Result

		for _, remote := range msg.HostIds {
			c.servers[remote] = newServer(remote)
		}

		c.self.Send(msgDialer{})
	case MsgPublish:
		if c.leaveFuture != nil {
			msg.Result.SetValue(ServerDisconnectedError)
			return
		}

		server := c.selectTopicServer(msg.Topic)
		if server == nil {
			msg.Result.SetValue(NoSubscribersError)
			return
		}
		msgId := NewId()
		time.AfterFunc(Timeout, func() {
			c.self.Send(reader.MsgMessageRead{HostId: server.serverId, Msg: &Message{
				MessageId:   msgId,
				MessageType: ReplyTimeout,
			}})
		})
		server.pendingReplies[msgId] = msg.Result
		server.writer.Send(&Message{
			Topic:       msg.Topic,
			Body:        msg.Body,
			MessageId:   msgId,
			MessageType: msg.MsgType,
		})
	case reader.MsgMessageRead:
		c.handleReadMessage(msg.HostId, msg.Msg)
	case writer.MsgMessageWritten:
		// TODO
	case MsgAddHost:
		if c.clientId == msg.HostId {
			return
		}
		if _, exists := c.servers[msg.HostId]; exists {
			return
		}
		c.servers[msg.HostId] = newServer(msg.HostId)
		c.self.Send(msgDial{})
	case msgDialer:
		c.self.Send(msgDial{})
		time.AfterFunc(RedialInterval, func() {
			c.self.Send(msgDialer{})
		})

	case msgDial:
		c.logf("msgDial: c.leaveFuture = %v; msg = %+v", c.leaveFuture, msg)
		if c.leaveFuture != nil {
			return
		}
		for _, server := range c.servers {
			if server.state == pendingDial || server.state == pendingRedial {
				server.state = dialing
				go dial(c.clientId, server.serverId, c.self, c.proxy)
			}
		}

	case msgDialError:
		server := c.servers[msg.serverId]
		if server == nil {
			return
		}
		server.state = pendingRedial
		Log.Errorf("Failed to dial %s: %v Will re-dial.", msg.serverId, msg.err)
		c.setDialResult()

	case msgDialed:
		c.logf("handleDialed: serverId = %s; conn = %s/%s", msg.serverId, msg.conn.LocalAddr(), msg.conn.RemoteAddr())

		server, ok := c.servers[msg.serverId]
		if !ok {
			Log.Errorf("Dialed to non-existing server: %s", msg.serverId)
			return
		}

		if server.state != dialing {
			Log.Errorf("Dialed to server %s in wrong state: %s", msg.serverId, server.state)
			return
		}

		server.state = dialed
		server.conn = msg.conn
		server.reader = reader.NewReader(msg.serverId, msg.conn, c.self)
		server.writer = writer.NewWriter(msg.serverId, msg.conn, c.self)

		buf := &bytes.Buffer{}
		Encode(c.clientId, buf)

		server.writer.Send(&Message{
			Body:        buf.Bytes(),
			MessageId:   NewId(),
			MessageType: Join,
		})
	case MsgLeave:
		// TODO: Implement
		c.logf("LEAVE: %v", msg)
		c.leaveFuture = msg.Result
		for _, server := range c.servers {
			c.shutdownServerInNeeded(server)
		}
		if len(c.servers) == 0 {
			c.leaveFuture.SetValue(false)
		} else {
			time.AfterFunc(Timeout, func() {
				c.forceShutdown()
			})
		}
	case MsgNetworkError:
		if c.leaveFuture != nil {
			return
		}
		// TODO
		panic(fmt.Sprintf("client cannot handle MsgNetworkError message [%T]: %+v", msg, msg))
	default:
		panic(fmt.Sprintf("client cannot handle message [%T]: %+v", msg, msg))
	}
}

func (c *client) forceShutdown() {
	for _, server := range c.servers {
		c.logf("###:2 Shutting down server %s", server)
		server.reader.Stop()
		server.writer.Stop()
		server.conn.Close()
	}
	if c.leaveFuture != nil {
		c.leaveFuture.SetValue(false)
	}
}

func dial(clientId, serverId HostId, requestor actor.Actor, _net proxy.Network) {
	conn, err := _net.Dial(clientId, serverId, RedialInterval*9/10)
	if err != nil {
		requestor.Send(msgDialError{serverId, err})
	} else {
		requestor.Send(msgDialed{serverId, conn})
	}

}

func (c *client) selectTopicServer(t Topic) *server {
	servers := c.getServersByTopic(t)
	if len(servers) == 0 {
		return nil
	}
	return servers[rand.Intn(len(servers))]
}

func (c *client) getServersByTopic(t Topic) []*server {
	result := []*server{}
	for _, server := range c.servers {
		if _, found := server.topics[t]; found {
			result = append(result, server)
		}
	}
	return result
}

func (c *client) handleReadMessage(from HostId, msg *Message) {
	server := c.servers[from]
	if server == nil {
		Log.Errorf("Received '%s' message from non-existing server %s. Ignored.", msg.MessageType, from)
		return
	}

	switch msg.MessageType {
	case Join:
		c.handleJoin(server, msg)
	case Reply:
		c.handleReply(server, msg)
	case ReplyTimeout:
		c.handleReplyError(server, msg, TimeoutError)
	case ReplyPanic:
		c.handleReplyError(server, msg, PanicError)
	case Subscribe:
		c.handleSubscribed(server, msg)
	case Unsubscribe:
		c.handleUnsubscribed(server, msg)
	case Leaving:
		c.handleLeaving(server, msg)
	default:
		panic(fmt.Sprintf("received message: %v", msg))
	}
	c.shutdownServerInNeeded(server)
}

func (c *client) handleJoin(server *server, msg *Message) {
	joinMessage := &JoinMessage{}
	Decode(bytes.NewBuffer(msg.Body), joinMessage)
	for _, topic := range joinMessage.Topics {
		server.topics[topic] = struct{}{}
	}
	server.state = connected
	c.logf("handleJoin: joinMessage = %v; peers = %d", joinMessage, len(joinMessage.Peers))
	for _, peer := range joinMessage.Peers {
		c.logf("handleJoin: dialing = %s", peer)
		if _, exists := c.servers[peer]; !exists {
			c.servers[peer] = newServer(peer)
		}
	}
	c.self.Send(msgDial{})
	c.setDialResult()
}

func (c *client) handleLeaving(server *server, msg *Message) {
	delete(c.servers, server.serverId)

	pendingFutures := make([]future.Future, len(server.pendingReplies))
	for _, pf := range server.pendingReplies {
		pendingFutures = append(pendingFutures, pf)
	}
	go waitReplies(pendingFutures)
}

func waitReplies(pendingFutures []future.Future) {
	for _, pf := range pendingFutures {
		pf.Value()
	}
}

func (c *client) setDialResult() {
	if c.dialResult == nil {
		return
	}
	for _, server := range c.servers {
		if server.state != connected && server.state != pendingRedial {
			return
		}
	}
	c.dialResult.SetValue(true)
	c.dialResult = nil
}

func (c *client) handleReply(server *server, msg *Message) {
	result := server.pendingReplies[msg.MessageId]
	delete(server.pendingReplies, msg.MessageId)
	if result == nil {
		Log.Errorf("Received unexpected reply for '%s'. Ignored.", msg.Topic)
		return
	}
	result.SetValue(msg)
}

func (c *client) handleReplyError(server *server, msg *Message, err error) {
	result := server.pendingReplies[msg.MessageId]
	delete(server.pendingReplies, msg.MessageId)
	result.SetValue(err)
}

func (c *client) shutdownServerInNeeded(server *server) {
	if c.leaveFuture != nil && len(server.pendingReplies) == 0 {
		c.logf("###:1 Shutting down server %s", server)
		if server.reader != nil {
			server.reader.Stop()
		}
		if server.writer != nil {
			server.writer.Stop()
		}
		if server.conn != nil {
			server.conn.Close()
		}
		delete(c.servers, server.serverId)
		if len(c.servers) == 0 {
			c.leaveFuture.SetValue(true)
		}
	}
}

func (c *client) handleSubscribed(server *server, msg *Message) {
	buf := bytes.NewBuffer(msg.Body)
	var t Topic
	Decode(buf, &t)
	server.topics[t] = struct{}{}
}

func (c *client) handleUnsubscribed(server *server, msg *Message) {
	buf := bytes.NewBuffer(msg.Body)
	var t Topic
	Decode(buf, &t)
	delete(server.topics, t)
}

func (c *client) handleNetworkError(_ string, info []interface{}) {
	panic("Implement me.")
	// serverId := info[0].(clientId)
	// err := info[1].(error)
	// if c.state == messengerLeaving {
	// 	c.Send("shutdown-server", serverId)
	// 	return
	// }
	// if server, found := c.servers[serverId]; found {
	// 	if server.state == serverStopping || server.state == serverLeaving {
	// 		return
	// 	}
	// 	server.state = serverStopping
	// 	if err.Error() == "EOF" {
	// 		Log.Errorf("Peer %s disconnected. Will try to re-connect.", serverId)
	// 	} else {
	// 		Log.Errorf("Peer %s: Network error: %v. Will try to re-connect.", serverId, err)
	// 	}

	// 	c.Send("shutdown-server", server.serverId)

	// 	if serverId > c.clientId {
	// 		time.AfterFunc(time.Millisecond, func() {
	// 			c.Send("dial", serverId)
	// 		})
	// 	} else {
	// 		time.AfterFunc(RedialInterval, func() {
	// 			c.Send("dial", serverId)
	// 		})
	// 	}
	// }
}

func (c *client) handleLeave(_ string, info []interface{}) {
	// panic("Implement me.")
	// c.leaveFuture = info[0].(future.Future)
	// if len(c.servers) == 0 {
	// 	c.leaveFuture.SetValue(true)
	// }
}

func (s *server) String() string {
	return fmt.Sprintf("server: state = %s", s.state)
}

func (s state) String() string {
	switch s {
	case pendingDial:
		return "pendingDial"
	case pendingRedial:
		return "pendingRedial"
	case dialing:
		return "dialing"
	case dialed:
		return "dialed"
	case connected:
		return "connected"
	default:
		panic(fmt.Sprintf("Unknown server state = %d", s))
	}
}

func (c *client) logf(format string, params ...interface{}) {
	Log.Debugf(">>> client-%s: "+format, append([]interface{}{c.clientId}, params...)...)
}
