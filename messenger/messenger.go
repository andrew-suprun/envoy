package messenger

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/andrew-suprun/envoy/actor"
	"github.com/andrew-suprun/envoy/future"
	mRand "math/rand"
	"net"
	"runtime/debug"
	"sync"
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

var (
	Timeout        time.Duration = 30 * time.Second
	RedialInterval time.Duration = 10 * time.Second
	Log            Logger        = &defaultLogger{}
)

type Messenger interface {
	Join(remotes ...string)
	Leave()

	Request(topic string, body []byte) ([]byte, MessageId, error)
	Survey(topic string, body []byte) ([][]byte, error)
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
	join
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
	peers         map[hostId]*peer
	listener      actor.Actor
	dialer        actor.Actor
}

type peer struct {
	peerId         hostId
	conn           net.Conn
	topics         map[topic]struct{}
	pendingReplies map[messageId]future.Future
	inflight       sync.WaitGroup
	reader         actor.Actor
	writer         actor.Actor
}

type message struct {
	MessageId   messageId   `codec:"id"`
	MessageType messageType `codec:"mt"`
	Topic       topic       `codec:"t,omitempty"`
	Body        []byte      `codec:"b,omitempty"`
}

type clientMessage struct {
	client actor.Actor
	*message
}

type joinMessage struct {
	HostId hostId   `codec:"h,omitempty"`
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
		peers:         make(map[hostId]*peer),
	}

	msgr.Actor = actor.NewActor(string(msgr.hostId)+"-messenger").
		RegisterHandler("dial", msgr.handleDial).
		RegisterHandler("dialed", msgr.handleConnected).
		RegisterHandler("accepted", msgr.handleConnected).
		RegisterHandler("write-result", msgr.handleWriteResult).
		RegisterHandler("subscribe", msgr.handleSubscribe).
		RegisterHandler("unsubscribe", msgr.handleUnsubscribe).
		RegisterHandler("send-message", msgr.handleSendMessage).
		RegisterHandler("message", msgr.handleMessage).
		RegisterHandler("network-error", msgr.handleNetworkError).
		RegisterHandler("leave", msgr.handleLeave)

	msgr.logf("NewMessenger: start listener")
	msgr.listener, err = newListener(string(msgr.hostId)+"-listener", msgr, msgr.newJoinMessage())
	if err != nil {
		msgr.Leave()
		return nil, err
	}

	msgr.logf("NewMessenger: start dialer")
	msgr.dialer = newDialer(string(msgr.hostId)+"-dialer", msgr)

	msgr.logf("NewMessenger: joined")
	return msgr, nil
}

func (msgr *messenger) Join(remotes ...string) {
	defer func() {
		msgr.logf("Join: done")
	}()
	for _, remote := range remotes {
		remoteAddr, err := resolveAddr(remote)
		if err == nil {
			result := future.NewFuture()
			msgr.logf("Join: dialing %s", remoteAddr)
			msgr.Send("dial", hostId(remoteAddr), result)
			result.Value()
			msgr.logf("Join: dialed %s", remoteAddr)
		} else {
			Log.Errorf("Cannot resolve address %s. Ignoring.", remote)
		}
	}

}

func (msgr *messenger) handleDial(_ string, info []interface{}) {
	addr := info[0].(hostId)
	result := info[1].(future.Future)
	msgr.dialer.Send("dial", addr, msgr.newJoinMessage(), result)
}

func (msgr *messenger) handleConnected(msgType string, info []interface{}) {
	addr := info[0].(hostId)
	conn := info[1].(net.Conn)
	reply := info[2].(*joinMessage)

	var result future.Future
	if len(info) > 3 && info[3] != nil {
		result = info[3].(future.Future)
	}
	msgr.logf("handleConnected: type = '%s'; addr = %s; conn = %s/%s; reply = %#v; result = %v",
		msgType, addr, conn.LocalAddr(), conn.RemoteAddr(), reply, result)
	defer func() {
		msgr.logf("handleConnected: done")
	}()

	if msgType == "accepted" {
		buf := &bytes.Buffer{}
		encode(msgr.newJoinMessage(), buf)
		msg := &message{
			MessageId:   newId(),
			MessageType: join,
			Body:        buf.Bytes(),
		}

		err := writeMessage(conn, msg)
		if err != nil {
			Log.Errorf("Failed to connect with '%s'.", addr)
			return
		}
	}

	_newPeer := msgr.newPeer(addr, conn, reply.Topics)
	if oldPeer, exists := msgr.peers[addr]; exists {
		if msgr.hostId < addr {
			delete(msgr.peers, addr)
			msgr.shutdown(oldPeer)
		} else {
			msgr.shutdown(_newPeer)
			return
		}
	}
	msgr.peers[addr] = _newPeer
	msgr.logf("new peer[%s]: %v", addr, _newPeer)

	newPeers := false
	for _, peerId := range reply.Peers {
		if _, found := msgr.peers[peerId]; !found {
			newPeers = true
			msgr.dialer.Send("dial", peerId, msgr.newJoinMessage(), result)
		}
	}

	if result != nil && !newPeers {
		result.SetValue(true)
	}
}

// todo: fugure out async shutdown
func (msgr *messenger) shutdown(peer *peer) {
	// todo: graceful shutdown
	peer.conn.Close()
}

func (msgr *messenger) newPeer(hostId hostId, conn net.Conn, topics []topic) *peer {
	peer := &peer{
		peerId:         hostId,
		conn:           conn,
		topics:         make(map[topic]struct{}),
		pendingReplies: make(map[messageId]future.Future),
	}
	for _, t := range topics {
		peer.topics[t] = struct{}{}
	}
	peer.reader = newReader(fmt.Sprintf("%s-%s-reader", msgr.hostId, hostId), hostId, conn, msgr)
	peer.writer = newWriter(fmt.Sprintf("%s-%s-writer", msgr.hostId, hostId), hostId, conn, msgr)
	return peer
}

func (msgr *messenger) handleMessage(_ string, info []interface{}) {
	from := info[0].(hostId)
	msg := info[1].(*message)

	peer := msgr.peers[from]
	if peer == nil {
		Log.Errorf("Received '%s' message for non-existing peer %s. Ignored.", msg.MessageType, from)
		return
	}

	msgr.logf("received message from %s: %v", from, msg)

	switch msg.MessageType {
	case request:
		msgr.handleRequest(peer, msg)
	case reply:
		msgr.handleReply(peer, msg)
	case subscribe:
		msgr.handleSubscribed(peer, msg)
	case unsubscribe:
		msgr.handleUnsubscribed(peer, msg)
	default:
		panic(fmt.Sprintf("received message: %v", msg))
	}
}

func (msgr *messenger) handleNetworkError(_ string, info []interface{}) {
	msgr.logf("handleNetworkError: info = %v", info)
}

func (msgr *messenger) handleRequest(peer *peer, msg *message) {
	handler := msgr.subscriptions[msg.Topic]
	if handler == nil {
		Log.Errorf("Received '%s' message for non-subscribed topic %s. Ignored.", msg.MessageType, msg.Topic)
		return
	}

	go msgr.runHandler(peer, msg, handler)
}

func (msgr *messenger) handleReply(peer *peer, msg *message) {
	result := peer.pendingReplies[msg.MessageId]
	if result == nil {
		Log.Errorf("Received unexpected reply for '%s'. Ignored.", msg.Topic)
		return
	}
	result.SetValue(msg)
}

func (msgr *messenger) handleSubscribed(peer *peer, msg *message) {
	buf := bytes.NewBuffer(msg.Body)
	var t topic
	decode(buf, &t)
	peer.topics[t] = struct{}{}
}

func (msgr *messenger) handleUnsubscribed(peer *peer, msg *message) {
	buf := bytes.NewBuffer(msg.Body)
	var t topic
	decode(buf, &t)
	delete(peer.topics, t)
}

func (msgr *messenger) runHandler(peer *peer, msg *message, handler Handler) {
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

	peer.writer.Send("write", reply)
}

func (msgr *messenger) runHandlerProtected(msg *message, handler Handler) (result []byte, err error) {
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

func (msgr *messenger) handleWriteResult(_ string, info []interface{}) {
	peerId := info[0].(hostId)
	msg := info[1].(*message)
	var err error
	if len(info) > 2 && info[2] != nil {
		err = info[2].(error)
	}

	peer := msgr.peers[peerId]
	if peer != nil {
		if err != nil {
			Log.Errorf("%s: network error on %s: %v", peerId, err)
			delete(msgr.peers, peerId)
			msgr.shutdown(peer)
		}

		if msg.MessageType == publish {
			if reply, exists := peer.pendingReplies[msg.MessageId]; exists {
				delete(peer.pendingReplies, msg.MessageId)
				reply.SetError(err)
			}
		}
	}
}

func (msgr *messenger) getTopics() []topic {
	topics := make([]topic, 0, len(msgr.subscriptions))
	for topic := range msgr.subscriptions {
		topics = append(topics, topic)
	}
	return topics
}

func (msgr *messenger) Subscribe(t string, handler Handler) {
	msgr.Send("subscribe", topic(t), handler)
}

func (msgr *messenger) handleSubscribe(_ string, info []interface{}) {
	topic := info[0].(topic)
	handler := info[1].(Handler)
	msgr.subscriptions[topic] = handler
	msgr.broadcastSubscription(subscribe, topic)
}

func (msgr *messenger) Unsubscribe(t string) {
	msgr.Send("unsubscribe", topic(t))
}

func (msgr *messenger) handleUnsubscribe(_ string, info []interface{}) {
	topic := info[0].(topic)
	delete(msgr.subscriptions, topic)
	msgr.broadcastSubscription(unsubscribe, topic)
}

func (msgr *messenger) broadcastSubscription(msgType messageType, t topic) {
	buf := &bytes.Buffer{}
	encode(t, buf)
	msgr.clientBroadcast(msgType, buf.Bytes())
}

func (msgr *messenger) clientBroadcast(msgType messageType, body []byte) {
	if len(msgr.peers) == 0 {
		return
	}

	msg := &message{
		MessageId:   newId(),
		MessageType: msgType,
		Topic:       "",
		Body:        body,
	}

	for _, to := range msgr.peers {
		to.writer.Send("write", msg)
	}
}

func (msgr *messenger) Leave() {
	msgr.Send("leave")
}

func (msgr *messenger) handleLeave(_ string, _ []interface{}) {
	msgr.listener.Send("stop")
	peers := msgr.peers
	msgr.peers = make(map[hostId]*peer)

	for _, peer := range peers {
		msgr.shutdown(peer)
	}
}

func (msgr *messenger) Publish(t string, body []byte) (MessageId, error) {
	_, msgId, err := msgr.sendMessage(topic(t), body, publish)
	return msgId, err
}

func (msgr *messenger) Request(t string, body []byte) ([]byte, MessageId, error) {
	return msgr.sendMessage(topic(t), body, request)
}

func (msgr *messenger) sendMessage(topic topic, body []byte, msgType messageType) ([]byte, MessageId, error) {
	msg := &message{
		MessageId:   newId(),
		MessageType: msgType,
		Topic:       topic,
		Body:        body,
	}
	reply := future.NewFuture()
	time.AfterFunc(Timeout, func() { reply.SetError(TimeoutError) })
	for {
		msgr.Send("send-message", msg, reply)
		replyMsg := reply.Value()
		err := reply.Error()
		if replyMsg != nil {
			return replyMsg.(*message).Body, msg.MessageId, nil
		} else if err == NoSubscribersError || err == TimeoutError {
			return nil, msg.MessageId, err
		}
	}
}

func (msgr *messenger) handleSendMessage(_ string, info []interface{}) {
	msg := info[0].(*message)
	reply := info[1].(future.Future)

	server := msgr.selectTopicServer(msg.Topic)
	if server == nil {
		reply.SetError(NoSubscribersError)
		return
	}
	server.pendingReplies[msg.MessageId] = reply
	server.writer.Send("write", msg)
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

	// TODO
	_ = msg

	return nil
}

func (msgr *messenger) selectTopicServer(t topic) *peer {
	servers := msgr.getServersByTopic(t)
	if len(servers) == 0 {
		return nil
	}
	return servers[mRand.Intn(len(servers))]
}

func (msgr *messenger) getServersByTopic(t topic) []*peer {
	result := []*peer{}
	for _, server := range msgr.peers {
		if _, found := server.topics[t]; found {
			result = append(result, server)
		}
	}
	return result
}

func (msgr *messenger) Survey(topic string, body []byte) ([][]byte, error) {
	// TODO
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
	case join:
		return "join"
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

func (h *peer) String() string {
	return fmt.Sprintf("[peer: id: %s; topics %d]", h.peerId, len(h.topics))
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

func (msgr *messenger) newJoinMessage() *joinMessage {
	joinMsg := &joinMessage{HostId: msgr.hostId}
	for t := range msgr.subscriptions {
		joinMsg.Topics = append(joinMsg.Topics, t)
	}
	for p := range msgr.peers {
		joinMsg.Peers = append(joinMsg.Peers, p)
	}

	return joinMsg
}

func (msgr *messenger) logf(format string, params ...interface{}) {
	Log.Debugf(">>> %s: "+format, append([]interface{}{string(msgr.hostId) + "-messenger"}, params...)...)
}
