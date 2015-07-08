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
	pendingReplies map[MsgId]*MsgPublish
	reader         actor.Actor
	writer         actor.Actor
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
		Msg    Message
		Result future.Future
	}
)

type (
	msgDialer    struct{}
	msgDial      struct{ onlyNew bool }
	msgDialError struct {
		serverId HostId
		err      error
	}
	msgDialed struct {
		serverId HostId
		conn     net.Conn
	}
	msgLeaveIfNedded struct{}
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
		pendingReplies: make(map[MsgId]*MsgPublish),
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
	case *MsgPublish:
		if c.leaveFuture != nil {
			msg.Result.SetValue(ServerDisconnectedError)
			return
		}

		server := c.selectTopicServer(msg.Msg.Topic)
		if server == nil {
			msg.Result.SetValue(NoSubscribersError)
			return
		}
		time.AfterFunc(Timeout, func() {
			c.self.Send(reader.MsgMessageRead{HostId: server.serverId, Msg: &Message{
				MessageId:   msg.Msg.MessageId,
				MessageType: ReplyTimeout,
			}})
		})
		server.pendingReplies[msg.Msg.MessageId] = msg
		server.writer.Send(&msg.Msg)
	case reader.MsgMessageRead:
		c.handleReadMessage(msg.HostId, msg.Msg)
	case writer.MsgMessageWritten:
		if server := c.servers[msg.HostId]; server != nil {
			if pr := server.pendingReplies[msg.Msg.MessageId]; pr != nil {
				pr.Result.SetValue(&Message{MessageId: msg.Msg.MessageId})
			}
		}
	case MsgAddHost:
		if c.clientId == msg.HostId {
			return
		}
		if _, exists := c.servers[msg.HostId]; exists {
			return
		}
		c.servers[msg.HostId] = newServer(msg.HostId)
		c.self.Send(msgDial{onlyNew: true})
	case msgDialer:
		c.self.Send(msgDial{onlyNew: true})
		time.AfterFunc(RedialInterval, func() {
			c.self.Send(msgDialer{})
		})

	case msgDial:
		if c.leaveFuture != nil {
			return
		}
		servers := map[HostId]*server{}
		for _, server := range c.servers {
			if server.serverId != c.clientId {
				if server.state == pendingDial || (server.state == pendingRedial && !msg.onlyNew) {
					servers[server.serverId] = server
				}
			}
		}
		serverIds := []HostId{}
		for serverIs := range servers {
			serverIds = append(serverIds, serverIs)
		}
		c.logf("msgDial.1: dialing %s", serverIds)

		for _, server := range servers {
			server.state = dialing
			go dial(c.clientId, server.serverId, c.self, c.proxy)
		}
		c.logf("msgDial.2: dialing %s", serverIds)

	case msgDialError:
		server := c.servers[msg.serverId]
		if server == nil {
			return
		}
		server.state = pendingRedial
		Log.Errorf("Failed to dial %s: %v Will re-dial.", msg.serverId, msg.err)
		c.setDialResult()

	case msgDialed:
		c.logf("msgDialed: dialed = %s", msg.serverId)
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
	case msgLeaveIfNedded:
		if c.leaveFuture != nil && len(c.servers) == 0 {
			c.leaveFuture.SetValue(false)
		}
	case MsgNetworkError:
		server := c.servers[msg.HostId]
		if server == nil {
			return
		}

		c.logf("MsgNetworkError: serverId = %s", msg.HostId)

		if server.reader != nil {
			server.reader.Send(actor.MsgStop{})
		}
		if server.writer != nil {
			server.writer.Send(actor.MsgStop{})
		}

		for _, pr := range server.pendingReplies {
			c.self.Send(pr)
		}

		if c.leaveFuture != nil {
			return
		}

		server = newServer(msg.HostId)
		c.servers[msg.HostId] = server
		c.self.Send(msgDial{onlyNew: true})

	default:
		panic(fmt.Sprintf("client %s cannot handle message [%T]: %+v", c.clientId, msg, msg))
	}
}

func (c *client) forceShutdown() {
	for _, server := range c.servers {
		server.reader.Send(actor.MsgStop{})
		server.writer.Send(actor.MsgStop{})
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
		if server.state == connected {
			if _, found := server.topics[t]; found {
				result = append(result, server)
			}
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
	c.logf("handleJoin: connected = %s", server.serverId)
	for _, peer := range joinMessage.Peers {
		if _, exists := c.servers[peer]; !exists {
			c.logf("handleJoin: newServer: %s", peer)
			c.servers[peer] = newServer(peer)
		}
	}
	c.self.Send(msgDial{onlyNew: true})
	c.setDialResult()
}

func (c *client) handleLeaving(server *server, msg *Message) {
	delete(c.servers, server.serverId)
	go waitReplies(server, c.self, c.clientId)
}

func waitReplies(server *server, self actor.Actor, clientId HostId) {
	time.AfterFunc(Timeout, func() {
		for _, pr := range server.pendingReplies {
			pr.Result.SetValue(TimeoutError)
		}
	})
	for _, pr := range server.pendingReplies {
		pr.Result.Value()
	}
	server.writer.Send(&Message{MessageType: Left})

	server.reader.Send(actor.MsgStop{})
	server.writer.Send(actor.MsgStop{})

	self.Send(msgLeaveIfNedded{})
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
	result, ok := server.pendingReplies[msg.MessageId]
	delete(server.pendingReplies, msg.MessageId)
	if !ok {
		Log.Errorf("Received unexpected reply for '%s'. Ignored.", msg.Topic)
		return
	}
	result.Result.SetValue(msg)
}

func (c *client) handleReplyError(server *server, msg *Message, err error) {
	result, ok := server.pendingReplies[msg.MessageId]
	delete(server.pendingReplies, msg.MessageId)
	if ok {
		result.Result.SetValue(err)
	}
}

func (c *client) shutdownServerInNeeded(server *server) {
	if c.leaveFuture != nil && len(server.pendingReplies) == 0 {
		if server.reader != nil {
			server.reader.Send(actor.MsgStop{})
		}
		if server.writer != nil {
			server.writer.Send(actor.MsgStop{})
		}
		delete(c.servers, server.serverId)
		if c.connectedServers() == 0 {
			c.leaveFuture.SetValue(true)
		}
	}
}

func (c *client) connectedServers() (servers int) {
	for _, server := range c.servers {
		if server.state == connected {
			servers++
		}
	}
	return servers
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

func (s *server) String() string {
	return fmt.Sprintf("[server %s: state: %s; topics: %v; pending: %d]", s.serverId, s.state, s.topics, len(s.pendingReplies))
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
