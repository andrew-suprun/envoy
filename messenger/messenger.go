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

	Publish(topic string, body []byte) (MessageId, error)
	Request(topic string, body []byte) ([]byte, MessageId, error)
	Broadcast(topic string, body []byte) (MessageId, error)
	Survey(topic string, body []byte) ([][]byte, MessageId, error)

	// No more then one subscription per topic.
	// Second subscription panics.
	Subscribe(topic string, handler Handler)
	Unsubscribe(topic string)
}

type MessageId interface {
	String() string
}

type Handler func(topic string, body []byte, msgId MessageId) []byte

const (
	publish messageType = iota
	request
	reply
	replyPanic
	join
	leaving
	left
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
	state         messengerState
	leaveFuture   future.Future
}

type messengerState int

const (
	messengerActive messengerState = iota
	messengerLeaving
)

type peer struct {
	msgr           actor.Actor
	msgrId         hostId
	peerId         hostId
	conn           net.Conn
	topics         map[topic]struct{}
	pendingReplies map[messageId]future.Future
	reader         actor.Actor
	writer         actor.Actor
	state          peerState
}

type peerState int

const (
	peerInitial peerState = iota
	peerConnected
	peerStopping
	peerLeaving
)

type message struct {
	MessageId   messageId   `codec:"id"`
	MessageType messageType `codec:"mt"`
	Topic       topic       `codec:"t,omitempty"`
	Body        []byte      `codec:"b,omitempty"`
}

type joinMessage struct {
	HostId hostId   `codec:"h,omitempty"`
	Topics []topic  `codec:"t,omitempty"`
	Peers  []hostId `codec:"p,omitempty"`
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
		RegisterHandler("send-message", msgr.handleSendMessage).
		RegisterHandler("broadcast-message", msgr.handleBroadcastMessage).
		RegisterHandler("message", msgr.handleMessage).
		RegisterHandler("network-error", msgr.handleNetworkError).
		RegisterHandler("dial-error", msgr.handleDialError).
		RegisterHandler("shutdown-peer", msgr.handleShutdownPeer).
		RegisterHandler("shutdown-messenger", msgr.handleShutdownMessenger).
		Start()

	msgr.dialer = newDialer(string(msgr.hostId)+"-dialer", msgr)

	msgr.listener, err = newListener(string(msgr.hostId)+"-listener", msgr, msgr.newJoinMessage())
	if err != nil {
		msgr.Leave()
		return nil, err
	}

	return msgr, nil
}

func (msgr *messenger) Join(remotes ...string) {
	for _, remote := range remotes {
		remoteAddr, err := resolveAddr(remote)
		if err == nil {
			result := future.NewFuture()
			msgr.Send("dial", hostId(remoteAddr), result)
			result.Value()
		} else {
			Log.Errorf("Cannot resolve address %s. Ignoring.", remote)
		}
	}
}

func (msgr *messenger) Leave() {
	msgr.leaveFuture = future.NewFuture()
	msgr.state = messengerLeaving
	msgr.broadcastMessage("", nil, leaving)
	msgr.listener.Stop()
	msgr.dialer.Stop()
	msgr.Send("shutdown-messenger")
	time.AfterFunc(Timeout, func() { msgr.leaveFuture.SetValue(false) })
	msgr.leaveFuture.Value()
	msgr.Stop()
}

func (msgr *messenger) Publish(t string, body []byte) (MessageId, error) {
	_, msgId, err := msgr.sendMessage(topic(t), body, publish)
	return msgId, err
}

func (msgr *messenger) Request(t string, body []byte) ([]byte, MessageId, error) {
	return msgr.sendMessage(topic(t), body, request)
}

func (msgr *messenger) Broadcast(t string, body []byte) (MessageId, error) {
	_, msgId, err := msgr.broadcastMessage(topic(t), body, publish)
	return msgId, err
}

func (msgr *messenger) Survey(t string, body []byte) ([][]byte, MessageId, error) {
	return msgr.broadcastMessage(topic(t), body, request)
}

func (msgr *messenger) Subscribe(_topic string, handler Handler) {
	msgr.subscriptions[topic(_topic)] = handler
	buf := &bytes.Buffer{}
	encode(_topic, buf)
	msgr.broadcastMessage("", buf.Bytes(), subscribe)
}

func (msgr *messenger) Unsubscribe(_topic string) {
	delete(msgr.subscriptions, topic(_topic))
	buf := &bytes.Buffer{}
	encode(_topic, buf)
	msgr.broadcastMessage("", buf.Bytes(), unsubscribe)
}

func (msgr *messenger) sendMessage(topic topic, body []byte, msgType messageType) ([]byte, MessageId, error) {
	msg := &message{
		MessageId:   newId(),
		MessageType: msgType,
		Topic:       topic,
		Body:        body,
	}
	for {
		reply := future.NewFuture()
		time.AfterFunc(Timeout, func() { reply.SetError(TimeoutError) })
		msgr.Send("send-message", msg, reply)
		replyMsg := reply.Value()
		err := reply.Error()
		if replyMsg != nil {
			return replyMsg.(*message).Body, msg.MessageId, nil
		} else if err == ServerDisconnectedError {
			continue
		} else {
			return nil, msg.MessageId, err
		}
	}
}

func (msgr *messenger) broadcastMessage(topic topic, body []byte, msgType messageType) ([][]byte, MessageId, error) {
	msg := &message{
		MessageId:   newId(),
		MessageType: msgType,
		Topic:       topic,
		Body:        body,
	}
	replies := future.NewFuture()
	msgr.Send("broadcast-message", msg, replies)
	responses := replies.Value().([]future.Future)
	time.AfterFunc(Timeout, func() {
		for _, reply := range responses {
			reply.SetError(TimeoutError)
		}
	})
	var bodies [][]byte
	var err error
	for _, reply := range responses {
		replyMsg := reply.Value()
		err := reply.Error()
		if replyMsg != nil {
			bodies = append(bodies, replyMsg.(*message).Body)
		} else if err == nil {
			err = reply.Error()
		}
	}
	return bodies, msg.MessageId, err
}

func (msgr *messenger) handleDial(_ string, info []interface{}) {
	peerId := info[0].(hostId)
	var result future.Future
	if len(info) > 1 {
		result = info[1].(future.Future)
	}
	if peerId != msgr.hostId {
		_, found := msgr.peers[peerId]
		if !found {
			msgr.peers[peerId] = msgr.newPeer(peerId)
			msgr.dialer.Send("dial", peerId, msgr.newJoinMessage(), result)
		}
	}
}

func (msgr *messenger) handleConnected(msgType string, info []interface{}) {
	conn := info[0].(net.Conn)
	reply := info[1].(*joinMessage)

	var result future.Future
	if len(info) > 3 && info[3] != nil {
		result = info[3].(future.Future)
		defer result.SetValue(true)
	}

	peer, found := msgr.peers[reply.HostId]

	if found && peer.state == peerConnected {
		conn.Close()
		return
	}

	if !found {
		peer = msgr.newPeer(reply.HostId)
		msgr.peers[reply.HostId] = peer
	}

	peer.setConn(conn).setTopics(reply.Topics)

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
			msgr.Send("shutdown-peer", peer.peerId)
			return
		}
	}

	peer.state = peerConnected
	Log.Infof("Peer %s joined. (%s)", peer.peerId, msgType)

	for _, peerId := range reply.Peers {
		if _, found := msgr.peers[peerId]; !found {
			msgr.Send("dial", peerId)
		}
	}
}

func (msgr *messenger) handleShutdownPeer(_ string, info []interface{}) {
	peerId := info[0].(hostId)
	peer := msgr.peers[peerId]
	if peer == nil {
		return
	}

	// Log.Debugf("stopPeer: disconnected: %d", len(peer.pendingReplies))
	for _, pending := range peer.pendingReplies {
		pending.SetError(ServerDisconnectedError)
	}
	delete(msgr.peers, peer.peerId)
	peer.pendingReplies = nil
	if peer.conn != nil {
		if peer.state == peerLeaving {
			peer.writer.Send("write", &message{MessageType: left})
		}
		peer.reader.Stop()
		peer.writer.Stop()
		peer.conn.Close()
	}
	msgr.Send("shutdown-messenger")
}

func (msgr *messenger) handleShutdownMessenger(msgType string, info []interface{}) {
	if msgr.state == messengerLeaving && msgr.leaveFuture != nil && len(msgr.peers) == 0 {
		msgr.leaveFuture.SetValue(true)
	}
}

func (msgr *messenger) newPeer(hostId hostId) *peer {
	peer := &peer{
		msgr:           msgr,
		msgrId:         msgr.hostId,
		peerId:         hostId,
		topics:         make(map[topic]struct{}),
		pendingReplies: make(map[messageId]future.Future),
	}
	return peer
}

func (peer *peer) setConn(conn net.Conn) *peer {
	peer.conn = conn
	peer.reader = newReader(fmt.Sprintf("%s-%s-reader", peer.msgrId, peer.peerId), peer.peerId, peer.conn, peer.msgr)
	peer.writer = newWriter(fmt.Sprintf("%s-%s-writer", peer.msgrId, peer.peerId), peer.peerId, peer.conn, peer.msgr)
	return peer
}

func (peer *peer) setTopics(topics []topic) *peer {
	for _, t := range topics {
		peer.topics[t] = struct{}{}
	}
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

	switch msg.MessageType {
	case publish, request:
		msgr.handleRequest(peer, msg)
	case reply:
		msgr.handleReply(peer, msg)
	case replyPanic:
		msgr.handleReplyPanic(peer, msg)
	case subscribe:
		msgr.handleSubscribed(peer, msg)
	case unsubscribe:
		msgr.handleUnsubscribed(peer, msg)
	case leaving:
		msgr.handleLeaving(peer, msg)
	case left:
		msgr.handleLeft(peer, msg)
	default:
		panic(fmt.Sprintf("received message: %v", msg))
	}
}

func (msgr *messenger) handleNetworkError(_ string, info []interface{}) {
	peerId := info[0].(hostId)
	err := info[1].(error)
	if msgr.state == messengerLeaving {
		msgr.Send("shutdown-peer", peerId)
		return
	}
	if peer, found := msgr.peers[peerId]; found {
		if peer.state == peerStopping || peer.state == peerLeaving {
			return
		}
		peer.state = peerStopping
		if err.Error() == "EOF" {
			Log.Errorf("Peer %s disconnected. Will try to re-connect.", peerId)
		} else {
			Log.Errorf("Peer %s: Network error: %v. Will try to re-connect.", peerId, err)
		}

		msgr.Send("shutdown-peer", peer.peerId)

		if peerId > msgr.hostId {
			time.AfterFunc(time.Millisecond, func() {
				msgr.Send("dial", peerId)
			})
		} else {
			time.AfterFunc(RedialInterval, func() {
				msgr.Send("dial", peerId)
			})
		}
	}
}

func (msgr *messenger) handleDialError(_ string, info []interface{}) {
	peerId := info[0].(hostId)
	peer := msgr.peers[peerId]
	if peer != nil {
		if peer.conn != nil {
			if peer.state == peerConnected {
				return
			}
		}
		msgr.Send("shutdown-peer", peer.peerId)
	}
	Log.Errorf("Failed to dial %s. Will re-dial.", peerId)
	time.AfterFunc(RedialInterval, func() {
		msgr.Send("dial", peerId)
	})
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
	delete(peer.pendingReplies, msg.MessageId)
	if result == nil {
		Log.Errorf("Received unexpected reply for '%s'. Ignored.", msg.Topic)
		return
	}
	result.SetValue(msg)
}

func (msgr *messenger) handleReplyPanic(peer *peer, msg *message) {
	result := peer.pendingReplies[msg.MessageId]
	delete(peer.pendingReplies, msg.MessageId)
	result.SetError(PanicError)
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

func (msgr *messenger) handleLeaving(peer *peer, msg *message) {
	peer.state = peerLeaving

	pendingFutures := make([]future.Future, len(peer.pendingReplies))
	for _, pf := range peer.pendingReplies {
		pendingFutures = append(pendingFutures, pf)
	}
	go stopPeer(peer, pendingFutures, msgr)
}

func (msgr *messenger) handleLeft(peer *peer, msg *message) {
	if peer == nil {
		return
	}

	peer.state = peerStopping
	pendingFutures := make([]future.Future, len(peer.pendingReplies))
	for _, pf := range peer.pendingReplies {
		pendingFutures = append(pendingFutures, pf)
	}
	go stopPeer(peer, pendingFutures, msgr)
}

func stopPeer(peer *peer, pendingFutures []future.Future, msgr actor.Actor) {
	for _, pf := range pendingFutures {
		if pf != nil {
			pf.Value()
		}
	}
	msgr.Send("shutdown-peer", peer.peerId)
	if peer.state == peerLeaving {
		Log.Infof("Peer %s left.", peer.peerId)
	}
	peer.writer.Send("write", &message{MessageType: left})

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

	result = handler(string(msg.Topic), msg.Body, msg.MessageId)
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
			msgr.Send("shutdown-peer", peer.peerId)
			if peerId > msgr.hostId {
				msgr.Send("dial", peerId)
			}
		}

		if msg.MessageType == publish || msg.MessageType == subscribe || msg.MessageType == unsubscribe {
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

func (msgr *messenger) handleBroadcastMessage(_ string, info []interface{}) {
	msg := info[0].(*message)
	replies := info[1].(future.Future)
	responses := make([]future.Future, 0, len(msgr.peers))

	for _, peer := range msgr.peers {
		if peer.state == peerConnected {
			response := future.NewFuture()
			responses = append(responses, response)
			peer.pendingReplies[msg.MessageId] = response
			peer.writer.Send("write", msg)
		}
	}
	replies.SetValue(responses)
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
		if server.state == peerConnected {
			if _, found := server.topics[t]; found {
				result = append(result, server)
			}
		}
	}
	return result
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

func (peer *peer) String() string {
	return fmt.Sprintf("[peer: id: %s; topics: %d; state: %s]", peer.peerId, len(peer.topics), peer.state)
}

func (s peerState) String() string {
	switch s {
	case peerInitial:
		return "initial"
	case peerConnected:
		return "connected"
	case peerStopping:
		return "stopping"
	case peerLeaving:
		return "leaving"
	default:
		panic(fmt.Errorf("Unknown peerState %d", s))
	}
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
