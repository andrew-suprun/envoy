package peers

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
	"math/rand"
	"net"
	"runtime/debug"
	"time"
)

const (
	publish MsgType = iota
	request
	reply
	broadcast
	survey
	replyPanic
	join
	dialRequest
	leaving
	left
	subscribe
	unsubscribe
)

const (
	messageIdSize = 16
)

type peers struct {
	actor.Actor
	hostId        HostId
	subscriptions map[Topic]Handler
	peers         map[HostId]*peer
	listener      Sender
	dialer        Sender
	netListener   net.Listener
	joinResult    future.Future
	leaveFuture   future.Future
}

type peer struct {
	peerId         HostId
	conn           net.Conn
	topics         map[Topic]struct{}
	pendingReplies map[MsgId]*pendingReply
	reader         Sender
	writer         Sender
	state          state
}

type state int

// TODO: review states
const (
	pendingDial state = iota
	pendingRedial
	connected
	peerStopping
	peerLeaving
)

type pendingReply struct {
	msg   *Message
	reply future.Future
}

func NewPeers(local string) (Sender, error) {
	hostId, err := resolveAddr(local)
	if err != nil {
		return nil, err
	}
	peers := &peers{
		hostId:        hostId,
		subscriptions: make(map[Topic]Handler),
		peers:         make(map[HostId]*peer),
	}
	peers.Actor = actor.NewActor(local+"-peers").
		RegisterHandler("join", peers.handleJoin).
		RegisterHandler("leave", peers.handleLeave).
		RegisterHandler("publish", peers.handlePublish).
		RegisterHandler("request", peers.handleRequest).
		RegisterHandler("broadcast", peers.handleBroadcast).
		RegisterHandler("survey", peers.handleSurvey).
		RegisterHandler("subscribe", peers.handleSubscribe).
		RegisterHandler("unsubscribe", peers.handleUnsubscribe).
		RegisterHandler("dial", peers.handleDial).
		RegisterHandler("dialed", peers.handleConnected).
		RegisterHandler("accepted", peers.handleConnected).
		RegisterHandler("timeout", peers.handleTimeout).
		RegisterHandler("message", peers.handleMessage).
		RegisterHandler("listening", peers.handleListening).
		RegisterHandler("network-error", peers.handleNetworkError).
		RegisterHandler("write-result", peers.handleWriteResult).
		RegisterHandler("dial-ticker", peers.handleDialTicker).

		// RegisterHandler("send-message", peers.handleSendMessage).
		// RegisterHandler("broadcast-message", peers.handleBroadcastMessage).
		// RegisterHandler("shutdown-peer", peers.handleShutdownPeer).
		// RegisterHandler("shutdown-peers", peers.handleShutdownMessenger).
		Start()
	return peers, nil
}

func (ps *peers) handleJoin(_ string, info []interface{}) {
	remotes, _ := info[0].([]string)
	ps.joinResult = info[1].(future.Future)

	var err error
	ps.listener, err = listener.NewListener(ps.hostId, ps, ps.newJoinMessage())
	if err != nil {
		ps.joinResult.SetError(err)
		ps.Send("leave", ps.joinResult)
	}

	for _, remote := range remotes {
		remoteAddr, err := resolveAddr(remote)
		if err == nil {
			ps.peers[remoteAddr] = newPeer(remoteAddr)
		} else {
			Log.Errorf("Cannot resolve address %s. Ignoring.", remote)
		}
	}
	ps.Send("dial-ticker")
}

func newPeer(peerId HostId) *peer {
	return &peer{
		peerId:         peerId,
		topics:         make(map[Topic]struct{}),
		pendingReplies: make(map[MsgId]*pendingReply),
		state:          pendingDial,
	}
}

func (ps *peers) handleDialTicker(_ string, _ []interface{}) {
	if ps.leaveFuture != nil {
		return
	}
	ps.Send("dial")
	time.Sleep(RedialInterval)
	ps.Send("dial-ticker")
}

func (ps *peers) handleDial(_ string, info []interface{}) {
	for _, peer := range ps.peers {
		if peer.state == pendingDial || peer.state == pendingRedial {
			if ps.hostId < peer.peerId {
				go dial(peer.peerId, ps.newJoinMessage(), ps)
			} else {
				go requestDial(peer.peerId, ps)
			}
		}
	}
}

func dial(hostId HostId, joinMsg *JoinMessage, recipient Sender) {
	conn, err := net.Dial("tcp", string(hostId))
	if err != nil {
		Log.Errorf("Failed to dial %s: %v. Will re-dial.", hostId, err)
		return
	}

	buf := &bytes.Buffer{}
	Encode(joinMsg, buf)
	msg := &Message{
		MessageType: Join,
		Body:        buf.Bytes(),
	}
	err = WriteMessage(conn, msg)
	if err != nil {
		conn.Close()
		Log.Errorf("Failed to dial %s: %v. Will re-dial.", hostId, err)
		return
	}

	replyMsg, err := ReadMessage(conn)
	if err != nil {
		conn.Close()
		Log.Errorf("Failed to dial %s: %v. Will re-dial.", hostId, err)
		return
	}

	buf = bytes.NewBuffer(replyMsg.Body)
	reply := &JoinMessage{}
	Decode(buf, reply)

	recipient.Send("dialed", conn, reply)
}

func requestDial(hostId HostId, recepient Sender) {
	conn, err := net.Dial("tcp", string(hostId))
	if err != nil {
		Log.Errorf("Failed to dial %s: %v. Will re-dial.", hostId, err)
		return
	}

	buf := &bytes.Buffer{}
	Encode(hostId, buf)
	reqDial := &Message{
		MessageType: RequestDial,
		Body:        buf.Bytes(),
	}
	err = WriteMessage(conn, reqDial)
	conn.Close()
	if err != nil {
		Log.Errorf("Failed to dial %s: %v. Will re-dial.", hostId, err)
	}
}

func (ps *peers) handleConnected(msgType string, info []interface{}) {
	conn := info[0].(net.Conn)
	reply := info[1].(*JoinMessage)

	peer := ps.peers[reply.HostId]
	if peer == nil {
		Log.Errorf("Connected from unknown peer %s. Ignoring.", reply.HostId)
		return
	}

	ps.setConn(peer, conn, reply.Topics)

	if msgType == "accepted" {
		buf := &bytes.Buffer{}
		Encode(ps.newJoinMessage(), buf)
		msg := &Message{
			MessageId:   NewId(),
			MessageType: join,
			Body:        buf.Bytes(),
		}

		err := WriteMessage(conn, msg)
		if err != nil {
			ps.Send("network-error", peer.peerId, err)
			return
		}
	}

	Log.Infof("Peer %s joined. (%s)", peer.peerId, msgType)

	for _, peerId := range reply.Peers {
		if _, found := ps.peers[peerId]; !found {
			ps.peers[peerId] = newPeer(peerId)
		}
	}
	if ps.joinResult != nil {
		for _, peer := range ps.peers {
			if peer.state == pendingDial {
				return
			}
		}
	}
	ps.joinResult.SetValue(true)
	ps.joinResult = nil
}

func (ps *peers) handleLeave(_ string, info []interface{}) {
	ps.logf("Leaving")
	leaveFuture, _ := info[0].(future.Future)
	time.AfterFunc(Timeout, func() { ps.forceShutdown() })
	ps.leaveFuture = leaveFuture
	ps.broadcastMessage("", nil, leaving, leaveFuture)
	if ps.listener != nil {
		ps.logf("Leaving listener")
		ps.listener.Send("leave")
	}
	if ps.netListener != nil {
		ps.netListener.Close()
	}
	if len(ps.peers) == 0 {
		ps.leaveFuture.SetValue(true)
		ps.Stop()
	}
}

func (ps *peers) forceShutdown() {
	for _, peer := range ps.peers {
		peer.reader.Send("leave")
		peer.writer.Send("leave")
		peer.conn.Close()
	}
	if ps.leaveFuture != nil {
		ps.leaveFuture.SetValue(false)
	}
}

func (ps *peers) handlePublish(_ string, info []interface{}) {
	t, _ := info[0].(string)
	body, _ := info[1].([]byte)
	reply, _ := info[2].(future.Future)
	ps.sendMessage(Topic(t), body, publish, reply)
}

func (ps *peers) handleRequest(_ string, info []interface{}) {
	t, _ := info[0].(string)
	body, _ := info[1].([]byte)
	reply, _ := info[2].(future.Future)
	ps.sendMessage(Topic(t), body, request, reply)
}

func (ps *peers) handleBroadcast(_ string, info []interface{}) {
	t, _ := info[0].(string)
	body, _ := info[1].([]byte)
	reply, _ := info[2].(future.Future)
	ps.broadcastMessage(Topic(t), body, broadcast, reply)
}

func (ps *peers) handleSurvey(_ string, info []interface{}) {
	t, _ := info[0].(string)
	body, _ := info[1].([]byte)
	reply, _ := info[2].(future.Future)
	ps.broadcastMessage(Topic(t), body, survey, reply)
}

func (ps *peers) handleSubscribe(_ string, info []interface{}) {
	t, _ := info[0].(string)
	handler, _ := info[1].(Handler)
	ps.subscriptions[Topic(t)] = handler
	buf := &bytes.Buffer{}
	Encode(t, buf)
	ps.broadcastMessageAsync("", buf.Bytes(), subscribe)
}

func (ps *peers) handleUnsubscribe(_ string, info []interface{}) {
	t, _ := info[0].(string)
	delete(ps.subscriptions, Topic(t))
	buf := &bytes.Buffer{}
	Encode(t, buf)
	ps.broadcastMessageAsync("", buf.Bytes(), unsubscribe)
}

func (ps *peers) sendMessage(topic Topic, body []byte, msgType MsgType, reply future.Future) {
	msg := &Message{
		MessageId:   NewId(),
		MessageType: msgType,
		Topic:       topic,
		Body:        body,
	}

	server := ps.selectTopicServer(msg.Topic)
	if server != nil {
		ps.sendMessageToServer(server, msg, reply)
	} else {
		reply.SetError(NoSubscribersError)
	}
}

func (ps *peers) sendMessageToServer(server *peer, msg *Message, reply future.Future) {
	server.pendingReplies[msg.MessageId] = &pendingReply{msg, reply}
	time.AfterFunc(Timeout, func() { ps.Send("timeout", server.peerId, msg.MessageId) })
	server.writer.Send("write", msg)
}

func (ps *peers) handleTimeout(_ string, info []interface{}) {
	peerId := info[0].(HostId)
	msgId := info[1].(MsgId)
	peer := ps.peers[peerId]
	if peer == nil {
		return
	}
	pr := peer.pendingReplies[msgId]
	if pr != nil {
		pr.reply.SetError(TimeoutError)
		delete(peer.pendingReplies, msgId)
	}
}

func (ps *peers) resendMessages(peer *peer) {
	for _, r := range peer.pendingReplies {
		if r.msg.MessageType == publish || r.msg.MessageType == request {
			server := ps.selectTopicServer(r.msg.Topic)
			if server == nil {
				r.reply.SetError(NoSubscribersError)
				continue
			}
			server.pendingReplies[r.msg.MessageId] = r
			server.writer.Send("write", r.msg)
		} else {
			r.reply.SetError(NoSubscribersError)
		}
	}
	peer.pendingReplies = make(map[MsgId]*pendingReply)
}

func (ps *peers) broadcastMessage(topic Topic, body []byte, msgType MsgType, reply future.Future) {
	msg := &Message{
		MessageId:   NewId(),
		MessageType: msgType,
		Topic:       topic,
		Body:        body,
	}

	responses := make([]future.Future, 0, len(ps.peers))

	for _, server := range ps.getServersByTopic(topic) {
		if server.state == connected {
			response := future.NewFuture()
			responses = append(responses, response)
			ps.sendMessageToServer(server, msg, response)
		}
	}
	reply.SetValue(responses)
}

func (ps *peers) broadcastMessageAsync(topic Topic, body []byte, msgType MsgType) {
	msg := &Message{
		MessageId:   NewId(),
		MessageType: msgType,
		Topic:       topic,
		Body:        body,
	}

	for _, peer := range ps.peers {
		if peer.state == connected {
			peer.writer.Send("write", msg)
		}
	}
}

func (ps *peers) handleShutdownPeer(_ string, info []interface{}) {
	peerId := info[0].(HostId)
	peer := ps.peers[peerId]
	if peer == nil {
		return
	}

	// Log.Debugf("stopPeer: disconnected: %d", len(peer.pendingReplies))
	for _, pending := range peer.pendingReplies {
		pending.reply.SetError(ServerDisconnectedError)
	}
	delete(ps.peers, peer.peerId)
	peer.pendingReplies = nil
	if peer.conn != nil {
		if peer.state == peerLeaving {
			peer.writer.Send("write", &Message{MessageType: left})
		}
		peer.reader.Send("leave")
		peer.writer.Send("leave")
		peer.conn.Close()
	}
	ps.Send("shutdown-peers")
}

func (ps *peers) setConn(peer *peer, conn net.Conn, topics []Topic) {
	peer.conn = conn
	peer.reader = reader.NewReader(fmt.Sprintf("%s-%s-reader", ps.hostId, peer.peerId), peer.peerId, peer.conn, ps)
	peer.writer = writer.NewWriter(fmt.Sprintf("%s-%s-writer", ps.hostId, peer.peerId), peer.peerId, peer.conn, ps)
	for _, t := range topics {
		peer.topics[t] = struct{}{}
	}
	peer.state = connected
}

func (ps *peers) handleListening(_ string, info []interface{}) {
	ps.netListener = info[0].(net.Listener)
}

func (ps *peers) handleMessage(_ string, info []interface{}) {
	from := info[0].(HostId)
	msg := info[1].(*Message)

	peer := ps.peers[from]
	if peer == nil {
		Log.Errorf("Received '%s' message for non-existing peer %s. Ignored.", msg.MessageType, from)
		return
	}

	switch msg.MessageType {
	case publish, request:
		ps.handleMessageRequest(peer, msg)
	case reply:
		ps.handleMessageReply(peer, msg)
	case replyPanic:
		ps.handleMessageReplyPanic(peer, msg)
	case subscribe:
		ps.handleMessageSubscribed(peer, msg)
	case unsubscribe:
		ps.handleMessageUnsubscribed(peer, msg)
	case leaving:
		ps.handleMessageLeaving(peer, msg)
	case left:
		ps.handleMessageLeft(peer, msg)
	default:
		panic(fmt.Sprintf("received message: %v", msg))
	}
}

func (ps *peers) handleNetworkError(_ string, info []interface{}) {
	peerId := info[0].(HostId)
	// err := info[1].(error)
	if ps.leaveFuture != nil {
		return
	}
	peer := ps.peers[peerId]
	delete(ps.peers, peerId)
	if peer != nil {
		ps.resendMessages(peer)
	}
	ps.Send("dial", peerId)

	// if peer, found := ps.peers[peerId]; found {
	// 	if peer.state == peerStopping || peer.state == peerLeaving {
	// 		return
	// 	}
	// 	peer.state = peerStopping
	// 	if err.Error() == "EOF" {
	// 		Log.Errorf("Peer %s disconnected. Will try to re-connect.", peerId)
	// 	} else {
	// 		Log.Errorf("Peer %s: Network error: %v. Will try to re-connect.", peerId, err)
	// 	}

	// 	ps.Send("shutdown-peer", peer.peerId)

	// 	if peerId > ps.hostId {
	// 		time.AfterFunc(time.Millisecond, func() {
	// 			ps.Send("dial", peerId)
	// 		})
	// 	} else {
	// 		time.AfterFunc(RedialInterval, func() {
	// 			ps.Send("dial", peerId)
	// 		})
	// 	}
	// }
}

func (ps *peers) handleMessageRequest(peer *peer, msg *Message) {
	handler := ps.subscriptions[msg.Topic]
	if handler == nil {
		Log.Errorf("Received '%s' message for non-subscribed topic %s. Ignored.", msg.MessageType, msg.Topic)
		return
	}

	go ps.runHandler(peer, msg, handler)
}

func (ps *peers) handleMessageReply(peer *peer, msg *Message) {
	result := peer.pendingReplies[msg.MessageId]
	delete(peer.pendingReplies, msg.MessageId)
	if result == nil {
		Log.Errorf("Received unexpected reply for '%s'. Ignored.", msg.Topic)
		return
	}
	result.reply.SetValue(msg)
}

func (ps *peers) handleMessageReplyPanic(peer *peer, msg *Message) {
	result := peer.pendingReplies[msg.MessageId]
	delete(peer.pendingReplies, msg.MessageId)
	result.reply.SetError(PanicError)
}

func (ps *peers) handleMessageSubscribed(peer *peer, msg *Message) {
	buf := bytes.NewBuffer(msg.Body)
	var t Topic
	Decode(buf, &t)
	peer.topics[t] = struct{}{}
}

func (ps *peers) handleMessageUnsubscribed(peer *peer, msg *Message) {
	buf := bytes.NewBuffer(msg.Body)
	var t Topic
	Decode(buf, &t)
	delete(peer.topics, t)
}

func (ps *peers) handleMessageLeaving(peer *peer, msg *Message) {
	peer.state = peerLeaving

	pendingFutures := make([]*pendingReply, len(peer.pendingReplies))
	for _, pf := range peer.pendingReplies {
		pendingFutures = append(pendingFutures, pf)
	}
	go stopPeer(peer, pendingFutures, ps)
}

func (ps *peers) handleMessageLeft(peer *peer, msg *Message) {
	if peer == nil {
		return
	}

	peer.state = peerStopping
	pendingFutures := make([]*pendingReply, len(peer.pendingReplies))
	for _, pf := range peer.pendingReplies {
		pendingFutures = append(pendingFutures, pf)
	}
	go stopPeer(peer, pendingFutures, ps)
}

func stopPeer(peer *peer, pendingFutures []*pendingReply, ps actor.Actor) {
	for _, pf := range pendingFutures {
		if pf.reply != nil {
			pf.reply.Value()
		}
	}
	ps.Send("shutdown-peer", peer.peerId)
	if peer.state == peerLeaving {
		Log.Infof("Peer %s left.", peer.peerId)
	}
	peer.writer.Send("write", &Message{MessageType: left})

}

func (ps *peers) runHandler(peer *peer, msg *Message, handler Handler) {
	result, err := ps.runHandlerProtected(msg, handler)
	if msg.MessageType == publish {
		return
	}

	reply := &Message{
		MessageId:   msg.MessageId,
		MessageType: reply,
		Body:        result,
	}

	if err == PanicError {
		reply.MessageType = replyPanic
	}

	peer.writer.Send("write", reply)
}

func (ps *peers) runHandlerProtected(msg *Message, handler Handler) (result []byte, err error) {
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

func (ps *peers) handleWriteResult(_ string, info []interface{}) {
	peerId := info[0].(HostId)
	msg := info[1].(*Message)
	peer := ps.peers[peerId]
	if peer != nil {
		if msg.MessageType == publish || msg.MessageType == broadcast || msg.MessageType == subscribe || msg.MessageType == unsubscribe {
			if reply, exists := peer.pendingReplies[msg.MessageId]; exists {
				delete(peer.pendingReplies, msg.MessageId)
				reply.reply.SetValue(msg.MessageId)
			}
		}
	}
}

func (ps *peers) getTopics() []Topic {
	topics := make([]Topic, 0, len(ps.subscriptions))
	for topic := range ps.subscriptions {
		topics = append(topics, topic)
	}
	return topics
}

func (ps *peers) selectTopicServer(t Topic) *peer {
	servers := ps.getServersByTopic(t)
	if len(servers) == 0 {
		return nil
	}
	return servers[rand.Intn(len(servers))]
}

func (ps *peers) getServersByTopic(topic Topic) []*peer {
	result := []*peer{}
	for _, peer := range ps.peers {
		if peer.state == connected {
			if _, found := peer.topics[topic]; found {
				result = append(result, peer)
			}
		}
	}
	return result
}

func (ps *peers) connId(conn net.Conn) string {
	if conn == nil {
		return fmt.Sprintf("%s/<nil>", ps.hostId)
	}
	return fmt.Sprintf("%s/%s->%s", ps.hostId, conn.LocalAddr(), conn.RemoteAddr())
}

func (peer *peer) String() string {
	return fmt.Sprintf("[peer: id: %s; topics: %d; state: %s]", peer.peerId, len(peer.topics), peer.state)
}

func (s state) String() string {
	switch s {
	case pendingDial:
		return "pendingDial"
	case pendingRedial:
		return "pendingRedial"
	case connected:
		return "connected"
	case peerStopping:
		return "peerStopping"
	case peerLeaving:
		return "peerLeaving"
	default:
		panic(fmt.Errorf("Unknown peerState %d", s))
	}
}

func resolveAddr(addr string) (HostId, error) {
	resolved, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return "", err
	}
	if resolved.IP == nil {
		return HostId(fmt.Sprintf("127.0.0.1:%d", resolved.Port)), nil
	}
	return HostId(resolved.String()), nil
}

func (ps *peers) newJoinMessage() *JoinMessage {
	joinMsg := &JoinMessage{HostId: ps.hostId}
	for t := range ps.subscriptions {
		joinMsg.Topics = append(joinMsg.Topics, t)
	}
	for p := range ps.peers {
		joinMsg.Peers = append(joinMsg.Peers, p)
	}

	return joinMsg
}

func (ps *peers) logf(format string, params ...interface{}) {
	Log.Debugf(">>> %s: "+format, append([]interface{}{string(ps.hostId) + "-peers"}, params...)...)
}
