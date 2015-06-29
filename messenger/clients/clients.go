package clients

import (
	"bytes"
	"fmt"
	"github.com/andrew-suprun/envoy/actor"
	"github.com/andrew-suprun/envoy/future"
	. "github.com/andrew-suprun/envoy/messenger"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"github.com/andrew-suprun/envoy/messenger/listener"
	"github.com/andrew-suprun/envoy/messenger/reader"
	"github.com/andrew-suprun/envoy/messenger/writer"
	"net"
	"runtime/debug"
)

type clients struct {
	actor.Actor
	hostId        HostId
	subscriptions map[Topic]Handler
	clients       map[HostId]*client
	listener      Sender
	servers       Sender
	leaveFuture   future.Future
}

type client struct {
	clientId       HostId
	topics         map[Topic]struct{}
	pendingReplies map[MsgId]future.Future
	reader         Sender
	writer         Sender
	conn           net.Conn
	state          state
}

type state int

const (
	accepted state = iota
)

func NewClients(hostId HostId, servers Sender) (Sender, error) {
	c := &clients{
		hostId:        hostId,
		subscriptions: make(map[Topic]Handler),
		clients:       make(map[HostId]*client),
		servers:       servers,
	}

	var err error
	c.listener, err = listener.NewListener(hostId, c)
	if err != nil {
		return nil, err
	}

	c.Actor = actor.NewActor("clients-"+string(hostId)).
		RegisterHandler("accepted", c.handleAccepted).
		RegisterHandler("subscribe", c.handleSubscribe).
		RegisterHandler("message", c.handleMessage).
		RegisterHandler("leave", c.handleLeave).
		Start()

	return c, nil
}

func (c *clients) SetServersActor(servers actor.Actor) {
	c.servers = servers
}

func (c *clients) handleAccepted(_ string, info []interface{}) {
	c.logf("handleAccepted: info = %#v", info)
	conn := info[0].(net.Conn)
	client := &client{
		clientId:       HostId(conn.RemoteAddr().String()),
		topics:         make(map[Topic]struct{}),
		pendingReplies: make(map[MsgId]future.Future),
		conn:           conn,
		state:          accepted,
		reader:         nil,
	}
	client.reader = reader.NewReader(fmt.Sprintf("clients-reader-%s-%s", c.hostId, client.clientId), client.clientId, conn, c)
	client.writer = writer.NewWriter(fmt.Sprintf("clients-writer-%s-%s", c.hostId, client.clientId), client.clientId, conn, c)

	c.clients[client.clientId] = client

	joinMsg := &JoinMessage{HostId: c.hostId}
	for topic := range c.subscriptions {
		joinMsg.Topics = append(joinMsg.Topics, topic)
	}
	for peer := range c.clients {
		joinMsg.Peers = append(joinMsg.Peers, peer)
	}

	buf := &bytes.Buffer{}
	Encode(joinMsg, buf)

	client.writer.Send("write", &Message{
		Body:        buf.Bytes(),
		MessageId:   NewId(),
		MessageType: Join,
	})
}

func (c *clients) handleMessage(_ string, info []interface{}) {
	from := info[0].(HostId)
	msg := info[1].(*Message)

	client := c.clients[from]
	if client == nil {
		Log.Errorf("Received '%s' message from non-existing client %s. Ignored.", msg.MessageType, from)
		c.logf("Clients %d.", len(c.clients))
		for clId := range c.clients {
			c.logf("Client %s.", clId)
		}
		return
	}

	switch msg.MessageType {
	case Join:
		c.handleJoin(client, msg)
	case Publish, Request:
		c.handleRequest(client, msg)
	case Leaving:
		c.handleLeaving(client, msg)
	default:
		panic(fmt.Sprintf("received message: %v", msg))
	}
}

func (c *clients) handleJoin(client *client, msg *Message) {
	var joinMsg JoinMessage
	Decode(bytes.NewBuffer(msg.Body), &joinMsg)
	c.logf("handleJoin: joinMsg = %v", joinMsg)
	c.servers.Send("dial", joinMsg.HostId)
}

func (c *clients) handleRequest(client *client, msg *Message) {
	handler := c.subscriptions[msg.Topic]
	if handler == nil {
		Log.Errorf("Received '%s' message for non-subscribed topic %s. Ignored.", msg.MessageType, msg.Topic)
		return
	}

	go c.runHandler(client, msg, handler)
}

func (c *clients) runHandler(client *client, msg *Message, handler Handler) {
	result, err := c.runHandlerProtected(msg, handler)
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

	client.writer.Send("write", reply)
}

func (c *clients) runHandlerProtected(msg *Message, handler Handler) (result []byte, err error) {
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

func (c *clients) handleNetworkError(_ string, info []interface{}) {
	panic("Implement me.")
	// clientId := info[0].(HostId)
	// err := info[1].(error)
	// if c.leaveFuture != nil {
	// 	c.Send("shutdown-client", clientId)
	// 	return
	// }
	// if client, found := c.clients[clientId]; found {
	// 	if client.state == clientStopping || client.state == clientLeaving {
	// 		return
	// 	}
	// 	client.state = clientStopping
	// 	if err.Error() == "EOF" {
	// 		Log.Errorf("Peer %s disconnected. Will try to re-connect.", clientId)
	// 	} else {
	// 		Log.Errorf("Peer %s: Network error: %v. Will try to re-connect.", clientId, err)
	// 	}

	// 	c.Send("shutdown-client", client.clientId)

	// 	if clientId > c.hostId {
	// 		time.AfterFunc(time.Millisecond, func() {
	// 			c.Send("dial", clientId)
	// 		})
	// 	} else {
	// 		time.AfterFunc(RedialInterval, func() {
	// 			c.Send("dial", clientId)
	// 		})
	// 	}
	// }
}

func (c *clients) handleLeaving(client *client, msg *Message) {
	panic("Implement me.")
	// client.state = clientLeaving

	// pendingFutures := make([]future.Future, len(client.pendingReplies))
	// for _, pf := range client.pendingReplies {
	// 	pendingFutures = append(pendingFutures, pf)
	// }
	// go stopPeer(client, pendingFutures, msgr)
}

func (c *clients) handleLeave(_ string, info []interface{}) {
	// c.broadcastMessage("", nil, Leaving)
	// c.listener.Stop()
	// c.self.Send("shutdown")
	// time.AfterFunc(timeout, func() { c.leaveFuture.SetValue(false) })
	// c.leaveFuture.Value()
	c.Send("stop")
}

func (c *clients) handleSubscribe(_ string, info []interface{}) {
	// panic("Implement me.")
	// msg := info[0].(*Message)
	// replies := info[1].(future.Future)
	// responses := make([]future.Future, 0, len(c.clients))

	// for _, client := range c.clients {
	// 	response := future.NewFuture()
	// 	responses = append(responses, response)
	// 	client.pendingReplies[msg.MessageId] = response
	// 	client.writer.Send("write", msg)
	// }
	// replies.SetValue(responses)
}

func (c *clients) logf(format string, params ...interface{}) {
	Log.Debugf(">>> clients-%s: "+format, append([]interface{}{c.hostId}, params...)...)
}
