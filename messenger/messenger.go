package messenger

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/andrew-suprun/envoy/actor"
	"log"
	mRand "math/rand"
	"net"
	"runtime/debug"
	"time"
)

var (
	ServerDisconnectedError = errors.New("server disconnected")
	TimeoutError            = errors.New("timed out")
	NoSubscribersError      = errors.New("no subscribers found")
	NoHandlerError          = errors.New("no handler for topic found")
	NilConnError            = errors.New("null connection")
	PanicError              = errors.New("server panic-ed")
)

var RedialInterval = 10 * time.Second
var Log Logger = &defaultLogger{}

type Messenger interface {
	Join(remotes ...string)
	Leave()

	Request(topic string, body []byte, timeout time.Duration) ([]byte, MessageId, error)
	Survey(topic string, body []byte, timeout time.Duration) ([][]byte, error)
	Publish(topic string, body []byte) (MessageId, error)
	Broadcast(topic string, body []byte) error

	// No more then one subscription per topic.
	// Second subscription panics.
	Subscribe(topic string, handler Handler)
	Unsubscribe(topic string)
}

type MessageId interface {
	String() string
}

type Handler func(topic string, body []byte) []byte

const (
	publish messageType = iota
	request
	reply
	replyPanic
	joinInvite
	joinAccept
	leaving
	subscribe
	unsubscribe
)

const (
	messageIdSize = 16
)

type (
	topic       string
	hostId      string
	messageId   [messageIdSize]byte
	messageType int
)

type messenger struct {
	actor.Actor
	hostId
	subscriptions map[topic]Handler
	servers       map[hostId]*serverActor
	clients       map[hostId]actor.Actor
	listener      actor.Actor
}

type serverActor struct {
	hostId
	actor.Actor
	topics map[topic]struct{}
}

type message struct {
	MessageId   messageId   `codec:"id"`
	MessageType messageType `codec:"mt"`
	Topic       topic       `codec:"t,omitempty"`
	Body        []byte      `codec:"b,omitempty"`
}

type actorMessage struct {
	*message
	error
}

type joinCommand struct {
	local        hostId
	remotes      []hostId
	responseChan chan error
}

type newPeerCommand struct {
	hostId
	net.Conn
}

type joinAcceptCommand struct {
	responseChan chan *joinAcceptBody
}

type joinAcceptReply struct {
	serverId hostId
	reply    *joinAcceptBody
}

type messageCommand struct {
	info   actor.Payload
	writer actor.Actor
}

type requestCommand struct {
	*message
	replyChan chan *actorMessage
}

type errorMessage struct {
	name string
	error
}

type joinAcceptBody struct {
	Topics []topic  `codec:"t,omitempty"`
	Peers  []hostId `codec:"p,omitempty"`
}

type subscribeMessageBody struct {
	HostId hostId `codec:"id,omitempty"`
	Topic  topic  `codec:"t,omitempty"`
}

func NewMessenger(local string) (Messenger, error) {
	localAddr, err := resolveAddr(local)
	if err != nil {
		return nil, err
	}

	msgr := &messenger{
		hostId:        hostId(localAddr),
		subscriptions: make(map[topic]Handler),
		clients:       make(map[hostId]actor.Actor),
		servers:       make(map[hostId]*serverActor),
	}

	msgr.listener, err = newListener(string(msgr.hostId)+"-listener", msgr, localAddr)
	if err != nil {
		msgr.stop()
		return nil, err
	}

	return msgr, nil
}

func (msgr *messenger) Join(remotes ...string) {
	var remoteAddrs []hostId
	for _, remote := range remotes {
		remoteAddr, err := resolveAddr(remote)
		if err == nil {
			remoteAddrs = append(remoteAddrs, remoteAddr)
		} else {
			Log.Errorf("Cannot resolve address %s. Ignoring.", remote)
		}
	}

	//
	//

	for _, remote := range remoteAddrs {
		msgr.startServer(remote)
	}

	// todo: start dialer

	//
	//

	msgr.Actor = actor.NewActor(string(msgr.hostId)+"-messenger").
		RegisterHandler("new-client", msgr.handleNewClient).
		RegisterHandler("join-accept", msgr.handleJoinAccept).
		RegisterHandler("join-accepted", msgr.handleJoinAccepted).
		RegisterHandler("message", msgr.handleMessage).
		RegisterHandler("error", msgr.handleError).
		// RegisterHandler("server-gone", msgr.handleServerGone).
		// RegisterHandler("client-gone", msgr.handleClientGone).
		Start()
}

func (msgr *messenger) startServer(serverId hostId) {
	if _, exists := msgr.servers[serverId]; !exists {
		server := newServer(fmt.Sprintf("%s-%s-server", msgr.hostId, serverId), serverId, msgr)
		msgr.servers[serverId] = &serverActor{
			hostId: serverId,
			Actor:  server,
			topics: make(map[topic]struct{}),
		}
		server.Send("start", nil)
	}
}

func (msgr *messenger) handleNewClient(_ actor.MessageType, info actor.Payload) {
	cmd := info.(*newPeerCommand)
	msgr.logf("received 'new-client': clientId = %s; conn = %s", cmd.hostId, cmd.Conn.RemoteAddr())
	if _, exists := msgr.clients[cmd.hostId]; !exists {
		client := newClient(fmt.Sprintf("%s-%s-client", msgr.hostId, cmd.hostId), cmd.hostId, cmd.Conn, msgr)
		msgr.clients[cmd.hostId] = client
		msgr.logf("started new client: clientId = %s; conn = %s", cmd.hostId, cmd.Conn.RemoteAddr())
	} else {
		msgr.logf("client already exists: clientId = %s; conn = %s", cmd.hostId, cmd.Conn.RemoteAddr())
	}
}

// todo: change responseChan to recipient actor
func (msgr *messenger) handleJoinAccept(_ actor.MessageType, info actor.Payload) {
	cmd := info.(joinAcceptCommand)
	var topics []topic = msgr.getTopics()

	var hostIds = map[hostId]struct{}{}
	for clientId := range msgr.clients {
		hostIds[clientId] = struct{}{}
	}
	for serverId := range msgr.servers {
		hostIds[serverId] = struct{}{}
	}
	remoteIds := make([]hostId, 0, len(hostIds))
	for remoteId := range hostIds {
		remoteIds = append(remoteIds, remoteId)
	}
	cmd.responseChan <- &joinAcceptBody{Topics: topics, Peers: remoteIds}
}

func (msgr *messenger) handleJoinAccepted(_ actor.MessageType, info actor.Payload) {
	msg := info.(*joinAcceptReply)
	msgr.logf("handleJoinAccepted: msg = %+v\n", msg)
	server := msgr.servers[msg.serverId]
	msgr.logf("handleJoinAccepted: server = %+v\n", server)
	if server != nil {
		for _, topic := range msg.reply.Topics {
			server.topics[topic] = struct{}{}
		}
	}
	for _, peer := range msg.reply.Peers {
		msgr.startServer(peer)
	}
}

func (msgr *messenger) handleMessage(_ actor.MessageType, info actor.Payload) {
	cmd := info.(*messageCommand)
	msg := cmd.info.(*actorMessage)
	handler := msgr.subscriptions[msg.Topic]
	if handler == nil {
		Log.Errorf("Received '%s' message for non-subscribed topic %s. Ignored.", msg.MessageType, msg.Topic)
		return
	}

	go msgr.runHandler(cmd, msg, handler)
}

func (msgr *messenger) runHandler(cmd *messageCommand, msg *actorMessage, handler Handler) {
	result, err := msgr.runHandlerProtected(msg, handler)
	if msg.MessageType == publish {
		return
	}

	reply := &message{
		MessageId:   msg.MessageId,
		MessageType: reply,
		Body:        result,
	}

	if err == PanicError {
		reply.MessageType = replyPanic
	}

	cmd.writer.Send("write", reply)
}

func (msgr *messenger) runHandlerProtected(msg *actorMessage, handler Handler) (result []byte, err error) {
	defer func() {
		recErr := recover()
		if recErr != nil {
			Log.Panic(recErr, string(debug.Stack()))
			result = nil
			err = PanicError
		}
	}()

	result = handler(string(msg.Topic), msg.Body)
	return result, err

}

func (msgr *messenger) handleError(_ actor.MessageType, info actor.Payload) {
	msg := info.(*errorMessage)
	msgr.logf("received error from %s: %v", msg.name, msg.error)
}

func (msgr *messenger) handleServerGone(_ actor.MessageType, info actor.Payload) {
	serverId := info.(hostId)
	delete(msgr.servers, serverId)
}

func (msgr *messenger) handleClientGone(_ actor.MessageType, info actor.Payload) {
	clientId := info.(hostId)
	delete(msgr.clients, clientId)
}

func (msgr *messenger) getTopics() []topic {
	topics := make([]topic, 0, len(msgr.subscriptions))
	for topic := range msgr.subscriptions {
		topics = append(topics, topic)
	}
	return topics
}

func (msgr *messenger) Subscribe(_topic string, handler Handler) {
	msgr.subscriptions[topic(_topic)] = handler
	msgr.broadcastSubscription(subscribe, _topic)
}

func (msgr *messenger) Unsubscribe(_topic string) {
	delete(msgr.subscriptions, topic(_topic))
	msgr.broadcastSubscription(unsubscribe, _topic)
}

func (msgr *messenger) Leave() {
	msgr.clientBroadcast(leaving, nil)

	msgr.listener.Stop()

	for _, server := range msgr.servers {
		server.Stop()
	}

	for _, client := range msgr.clients {
		client.Stop()
	}

	// todo: wait for client and servers to be gone
}

func (msgr *messenger) broadcastSubscription(msgType messageType, topic string) {
	buf := &bytes.Buffer{}
	encode(topic, buf)
	msgr.clientBroadcast(msgType, buf.Bytes())
}

func (msgr *messenger) Publish(_topic string, body []byte) (MessageId, error) {
	to := msgr.selectTopicServer(topic(_topic))
	if to == nil {
		return newId(), NoSubscribersError
	}

	msg := &message{
		MessageId:   newId(),
		MessageType: publish,
		Topic:       topic(_topic),
		Body:        body,
	}
	to.Send("publish", msg)
	return msg.MessageId, nil
}

func (msgr *messenger) Broadcast(_topic string, body []byte) error {
	servers := msgr.getServersByTopic(topic(_topic))
	if len(servers) == 0 {
		return NoSubscribersError
	}

	msg := &message{
		MessageId:   newId(),
		MessageType: publish,
		Topic:       topic(_topic),
		Body:        body,
	}

	for _, server := range servers {
		server.Send("publish", msg)
	}

	return nil
}

func (msgr *messenger) clientBroadcast(msgType messageType, body []byte) {
	msg := &message{
		MessageId:   newId(),
		MessageType: msgType,
		Topic:       "",
		Body:        body,
	}

	for _, client := range msgr.clients {
		client.Send("write", msg)
	}
}

func (msgr *messenger) Request(_topic string, body []byte, timeout time.Duration) ([]byte, MessageId, error) {
	timeoutChan := time.After(timeout)

	msg := &message{
		MessageId:   newId(),
		MessageType: request,
		Topic:       topic(_topic),
		Body:        body,
	}

	for {
		server := msgr.selectTopicServer(topic(_topic))
		if server == nil {
			return []byte{}, msg.MessageId, NoSubscribersError
		}
		replyChan := make(chan *actorMessage)
		server.Send("request", &requestCommand{msg, replyChan})
		select {
		case <-timeoutChan:
			return nil, msg.MessageId, TimeoutError
		case reply := <-replyChan:
			if reply.error == ServerDisconnectedError {
				delete(msgr.servers, server.hostId)
				continue
			}
			return reply.message.Body, msg.MessageId, reply.error
		}
	}
}

func (msgr *messenger) selectTopicServer(t topic) *serverActor {
	servers := msgr.getServersByTopic(t)
	if len(servers) == 0 {
		return nil
	}
	return servers[mRand.Intn(len(servers))]
}

func (msgr *messenger) getServersByTopic(t topic) []*serverActor {
	result := []*serverActor{}
	for _, server := range msgr.servers {
		if _, found := server.topics[t]; found {
			result = append(result, server)
		}
	}
	return result
}

func (msgr *messenger) Survey(topic string, body []byte, timeout time.Duration) ([][]byte, error) {
	return nil, nil
}

func (msgr *messenger) connId(conn net.Conn) string {
	if conn == nil {
		return fmt.Sprintf("%s/<nil>", msgr.hostId)
	}
	return fmt.Sprintf("%s/%s->%s", msgr.hostId, conn.LocalAddr(), conn.RemoteAddr())
}

func newId() (mId messageId) {
	rand.Read(mId[:])
	return
}

func (mId messageId) String() string {
	return hex.EncodeToString(mId[:])
}

func (mType messageType) String() string {
	switch mType {
	case publish:
		return "publish"
	case request:
		return "request"
	case reply:
		return "reply"
	case replyPanic:
		return "replyPanic"
	case joinInvite:
		return "joinInvite"
	case joinAccept:
		return "joinAccept"
	case leaving:
		return "leaving"
	case subscribe:
		return "subscribe"
	case unsubscribe:
		return "unsubscribe"
	default:
		panic(fmt.Errorf("Unknown messageType %d", mType))
	}
}

func (h *client) String() string {
	return fmt.Sprintf("[client: id: %s]", h.hostId)
}

func (h *serverActor) String() string {
	return fmt.Sprintf("[server: id: %s; topics %d]", h.hostId, len(h.topics))
}

func (msg *message) String() string {
	if msg == nil {
		return "[message: nil]"
	}
	if msg.Body != nil {
		return fmt.Sprintf("[message[%s/%s]: topic: %s; body.len: %d]", msg.MessageId, msg.MessageType, msg.Topic, len(msg.Body))
	}
	return fmt.Sprintf("[message[%s/%s]: topic: %s; body: <nil>]", msg.MessageId, msg.MessageType, msg.Topic)
}

func (pr *actorMessage) String() string {
	return fmt.Sprintf("[actorMessage: msg: %s; err = %v]", pr.message, pr.error)
}

func resolveAddr(addr string) (hostId, error) {
	resolved, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return "", err
	}
	if resolved.IP == nil {
		return hostId(fmt.Sprintf("127.0.0.1:%d", resolved.Port)), nil
	}
	return hostId(resolved.String()), nil
}

func (msgr *messenger) stop() {
	msgr.Stop()
}

func (msgr *messenger) logf(format string, params ...interface{}) {
	log.Printf(">>> %s: "+format, append([]interface{}{string(msgr.hostId) + "-messenger"}, params...)...)
}
