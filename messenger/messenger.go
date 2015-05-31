package messenger

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ugorji/go/codec"
	mRand "math/rand"
	"net"
	"runtime/debug"
	"sync"
	"time"
)

var (
	FailedToJoinError       = errors.New("failed to join")
	ServerDisconnectedError = errors.New("server disconnected")
	TimeoutError            = errors.New("timed out")
	NoSubscribersError      = errors.New("no subscribers found")
	NoHandlerError          = errors.New("no handler for topic found")
	NilConnError            = errors.New("null connection")
	PanicError              = errors.New("server panic-ed")
)

var RedialInterval = 10 * time.Second

type Messenger interface {
	Join(local string, remotes ...string) error
	Leave()

	Request(topic string, body []byte, timeout time.Duration) ([]byte, error)
	Survey(topic string, body []byte, timeout time.Duration) ([][]byte, error)
	Publish(topic string, body []byte) error
	Broadcast(topic string, body []byte) error

	// No more then one subscription per topic.
	// Second subscription panics.
	Subscribe(topic string, handler Handler)
	Unsubscribe(topic string)

	SetLogger(logger Logger)
}

type Handler func(topic string, body []byte) []byte

//
// impl
//

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
	topic          string
	hostId         string
	messageId      [messageIdSize]byte
	messageType    int
	handlers       map[topic]Handler
	pendingReplies map[messageId]chan *replyMessage
)

type messenger struct {
	hostId
	net.Listener
	Logger
	subscriptions
	servers
	clients
	dialMutex       sync.Mutex
	testReadMessage func(conn net.Conn)
	closing         bool
}

func NewMessenger() Messenger {
	return &messenger{
		Logger:          defaultLogger,
		subscriptions:   subscriptions{handlers: make(handlers)},
		clients:         clients{hosts: make(map[hostId]*client)},
		servers:         servers{hosts: make(map[hostId]*server)},
		testReadMessage: func(conn net.Conn) {},
	}
}

type subscriptions struct {
	sync.Mutex
	handlers
}

type servers struct {
	sync.Mutex
	hosts map[hostId]*server
}

type clients struct {
	sync.Mutex
	hosts map[hostId]*client
}

type server struct {
	sync.Mutex
	hostId
	conn   net.Conn
	topics map[topic]struct{}
	pendingReplies
	serverState
}

type serverState int

const (
	serverConnected serverState = iota
	serverLeaving
	serverLeft
	serverDisconnected
	serverDoesNotExist
)

type client struct {
	sync.Mutex
	hostId
	conn net.Conn
}

type message struct {
	MessageId   messageId   `codec:"id"`
	MessageType messageType `codec:"mt"`
	Topic       topic       `codec:"t,omitempty"`
	Body        []byte      `codec:"b,omitempty"`
}

type replyMessage struct {
	*message
	error
}

// type joinInviteBody struct {
// 	HostId hostId `codec:"id,omitempty"`
// }

type joinAcceptBody struct {
	Topics []topic  `codec:"t,omitempty"`
	Peers  []hostId `codec:"p,omitempty"`
}

type subscribeMessageBody struct {
	HostId hostId `codec:"id,omitempty"`
	Topic  topic  `codec:"t,omitempty"`
}

func (msgr *messenger) getTopics() []topic {
	msgr.subscriptions.Lock()
	topics := make([]topic, 0, len(msgr.subscriptions.handlers))
	for topic := range msgr.subscriptions.handlers {
		topics = append(topics, topic)
	}
	msgr.subscriptions.Unlock()
	return topics
}

func (msgr *messenger) getHandler(t topic) Handler {
	msgr.subscriptions.Lock()
	handler := msgr.subscriptions.handlers[t]
	msgr.subscriptions.Unlock()
	return handler
}

func (msgr *messenger) Subscribe(_topic string, handler Handler) {
	msgr.subscriptions.Lock()
	msgr.subscriptions.handlers[topic(_topic)] = handler
	msgr.subscriptions.Unlock()
	msgr.broadcastSubscribtion(subscribe, _topic)
}

func (msgr *messenger) Unsubscribe(_topic string) {
	msgr.subscriptions.Lock()
	delete(msgr.subscriptions.handlers, topic(_topic))
	msgr.subscriptions.Unlock()
	msgr.broadcastSubscribtion(unsubscribe, _topic)
}

func (msgr *messenger) newClient(clientId hostId, conn net.Conn) {
	client := &client{
		hostId: clientId,
		conn:   conn,
	}
	msgr.clients.Mutex.Lock()
	msgr.clients.hosts[clientId] = client
	msgr.clients.Unlock()

	go msgr.clientReadLoop(client.hostId)
}

func (msgr *messenger) getClientConn(clientId hostId) (result net.Conn) {
	msgr.clients.Mutex.Lock()
	client, ok := msgr.clients.hosts[clientId]
	if ok {
		result = client.conn
	}
	msgr.clients.Unlock()
	return result
}

func (msgr *messenger) removeClient(clientId hostId) {
	msgr.clients.Mutex.Lock()
	delete(msgr.clients.hosts, clientId)
	msgr.clients.Unlock()
}

func (msgr *messenger) getClientIds() []hostId {
	msgr.clients.Lock()
	result := make([]hostId, 0, len(msgr.clients.hosts))
	for clientId := range msgr.clients.hosts {
		result = append(result, clientId)
	}
	msgr.clients.Unlock()
	return result
}

func (msgr *messenger) newServer(serverId hostId, conn net.Conn, topics []topic) {
	msgr.removeServer(serverId)

	server := &server{
		hostId:         serverId,
		conn:           conn,
		topics:         mapTopics(topics),
		pendingReplies: make(pendingReplies),
		serverState:    serverConnected,
	}

	msgr.servers.Lock()
	msgr.servers.hosts[serverId] = server
	msgr.servers.Unlock()
	msgr.Debugf("%s: newServer: %s", msgr.connId(conn), server)

	go msgr.serverReadLoop(serverId)
}

func (msgr *messenger) getServerState(serverId hostId) serverState {
	msgr.servers.Mutex.Lock()
	defer msgr.servers.Unlock()

	server := msgr.servers.hosts[serverId]
	if server == nil {
		return serverDoesNotExist
	}
	return server.serverState
}

func (msgr *messenger) setServerState(serverId hostId, state serverState) {
	msgr.servers.Mutex.Lock()

	server := msgr.servers.hosts[serverId]
	if server != nil {
		server.serverState = state
	}

	msgr.servers.Unlock()
}

func (msgr *messenger) getServerIds() []hostId {
	msgr.servers.Mutex.Lock()
	result := make([]hostId, 0, len(msgr.servers.hosts))
	for id, server := range msgr.servers.hosts {
		if server.serverState == serverConnected {
			result = append(result, id)
		}
	}
	msgr.servers.Unlock()
	return result
}

func (msgr *messenger) getServerIdsByTopic(_topic topic) (result []hostId) {
	msgr.servers.Mutex.Lock()
	for id, server := range msgr.servers.hosts {
		if server.serverState == serverConnected {
			server.Lock()
			if _, found := server.topics[_topic]; found {
				result = append(result, id)
			}
			server.Unlock()
		}
	}
	msgr.servers.Unlock()
	return result
}

func (msgr *messenger) getServerConn(serverId hostId) (result net.Conn) {
	msgr.servers.Mutex.Lock()
	server, ok := msgr.servers.hosts[serverId]
	if ok {
		result = server.conn
	}
	msgr.servers.Unlock()
	return result
}

func (msgr *messenger) removeServer(serverId hostId) {
	msgr.servers.Mutex.Lock()
	defer msgr.servers.Unlock()

	server := msgr.servers.hosts[serverId]
	if server == nil {
		return
	}
	delete(msgr.servers.hosts, serverId)
	server.Lock()
	if server != nil {
		for _, replyChan := range server.pendingReplies {
			replyChan <- &replyMessage{error: ServerDisconnectedError}
		}
	}
	if server.conn != nil {
		server.conn.Close()
	}
	server.Unlock()
}

func (msgr *messenger) addServerTopic(serverId hostId, _topic topic) {
	msgr.servers.Lock()
	server := msgr.servers.hosts[serverId]
	msgr.servers.Unlock()
	if server == nil {
		return
	}
	server.Lock()
	server.topics[_topic] = struct{}{}
	server.Unlock()
}

func (msgr *messenger) removeServerTopic(serverId hostId, _topic topic) {
	msgr.servers.Lock()
	server := msgr.servers.hosts[serverId]
	msgr.servers.Unlock()
	if server == nil {
		return
	}
	server.Lock()
	delete(server.topics, _topic)
	server.Unlock()
}

func (msgr *messenger) setReplyChan(serverId hostId, msgId messageId) chan *replyMessage {
	msgr.servers.Lock()
	server := msgr.servers.hosts[serverId]
	msgr.servers.Unlock()
	if server == nil {
		return nil
	}
	result := make(chan *replyMessage)
	server.Lock()
	server.pendingReplies[msgId] = result
	server.Unlock()
	return result
}

func (msgr *messenger) getReplyChan(serverId hostId, msgId messageId) chan *replyMessage {
	msgr.servers.Lock()
	server := msgr.servers.hosts[serverId]
	msgr.servers.Unlock()
	if server == nil {
		return nil
	}
	server.Lock()
	result := server.pendingReplies[msgId]
	server.Unlock()
	return result
}

func (msgr *messenger) removeReplyChan(serverId hostId, msgId messageId) {
	msgr.servers.Lock()
	server := msgr.servers.hosts[serverId]
	msgr.servers.Unlock()
	if server == nil {
		return
	}
	server.Lock()
	delete(server.pendingReplies, msgId)
	server.Unlock()
}

func (msgr *messenger) Join(local string, remotes ...string) error {
	msgr.Debugf("Join: local = %s; remotes = %s", local, remotes)

	addr, err := resolveAddr(local)
	if err != nil {
		return err
	}
	local = string(addr)

	msgr.Listener, err = net.Listen("tcp", local)
	if err != nil {
		msgr.Errorf("Failed to listen on %s", local)
		return err
	}
	msgr.hostId = hostId(msgr.Listener.Addr().String())
	msgr.Infof("Listening on: %s", msgr.hostId)

	go msgr.acceptConnections()

	joined := len(remotes) == 0
	for _, remote := range remotes {
		pendingHost, err := resolveAddr(remote)
		if err != nil {
			msgr.Errorf("%s: Cannot resolve address %s. Ignoring.", remote)
		}
		err = msgr.dial(hostId(pendingHost))
		joined = joined || err == nil
	}

	if !joined {
		return FailedToJoinError
	}

	for {
		if len(msgr.getClientIds()) == len(msgr.getServerIds()) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

func (msgr *messenger) scheduleRedial(serverId hostId) {
	time.AfterFunc(RedialInterval, func() {
		msgr.dial(serverId)
	})
}

func (msgr *messenger) dial(serverId hostId) error {
	if msgr.closing {
		return nil
	}

	msgr.Debugf("%s: dialing %s", msgr.hostId, serverId)

	if msgr.hostId == serverId {
		msgr.Debugf("%s: dial.10: Won't dial self. Skipping.", msgr.hostId)
		return nil
	}

	msgr.dialMutex.Lock()
	defer msgr.dialMutex.Unlock()

	state := msgr.getServerState(serverId)
	msgr.Debugf("%s: dial.20: server state = %s", msgr.hostId, state)

	if state == serverLeaving {
		msgr.Debugf("%s: dial.30: server %s is leaving.", msgr.hostId, serverId)
		return nil
	}

	if state == serverConnected {
		msgr.Debugf("%s: dial.40: host %s is already connected.", msgr.hostId, serverId)
		return nil
	}

	conn, err := net.Dial("tcp", string(serverId))
	msgr.Debugf("%s: dial.50: err = %v", msgr.connId(conn), err)
	if err != nil {
		msgr.Errorf("%s: Failed to connect to '%s'. Will re-try.", msgr.hostId, serverId)
		msgr.scheduleRedial(serverId)
		return err
	}

	err = msgr.writeJoinInvite(conn)
	msgr.Debugf("%s: dial.60: wrote invite; err = %v", msgr.connId(conn), err)
	if err != nil {
		msgr.Errorf("%s: Failed to invite '%s'. Will re-try.", msgr.hostId, serverId)
		msgr.scheduleRedial(serverId)
		return err
	}

	reply, err := msgr.readJoinAccept(conn)
	msgr.Debugf("%s: dial.60: read accept; err = %v", msgr.connId(conn), err)
	if err != nil {
		msgr.Errorf("Failed to read join accept from '%s'. Will re-try.", conn)
		msgr.scheduleRedial(serverId)
		return err
	}

	msgr.Debugf("%s: dial.70: create new server", msgr.connId(conn))
	msgr.newServer(serverId, conn, reply.Topics)

	msgr.Debugf("%s: dial.80: dialing = %v", msgr.connId(conn), reply.Peers)
	for _, hostId := range reply.Peers {
		go msgr.dial(hostId)
	}

	// msgr.Infof("%s: Joined by %s; clients = %s; servers = %s", msgr.hostId, serverId, msgr.getClientIds(), msgr.getServerIds())

	return nil
}

func (msgr *messenger) acceptConnections() {
	for !msgr.closing {
		conn, err := msgr.Listener.Accept()
		if err != nil {
			if msgr.closing {
				return
			}
			msgr.Errorf("%s: Failed to accept connection: %s", msgr.connId(conn), err)
			continue
		}

		clientId, err := msgr.readJoinInvite(conn)
		if err != nil {
			msgr.Errorf("Failed to read join invite to '%s'. Will re-try.", clientId)
			msgr.scheduleRedial(clientId)
			return
		}

		err = msgr.writeJoinAccept(conn)
		if err != nil {
			msgr.Errorf("Failed to write join accept to '%s'. Will re-try.", clientId)
			msgr.scheduleRedial(clientId)
			return
		}

		msgr.newClient(clientId, conn)
		go msgr.dial(clientId)

		msgr.Infof("%s: %s has joined.", msgr.hostId, clientId)
	}
}

func sliceTopics(topicMap map[topic]struct{}) []topic {
	result := make([]topic, len(topicMap))
	for topic := range topicMap {
		result = append(result, topic)
	}
	return result
}

func mapTopics(topicSlice []topic) map[topic]struct{} {
	result := make(map[topic]struct{}, len(topicSlice))
	for _, topic := range topicSlice {
		result[topic] = struct{}{}
	}
	return result
}

func (msgr *messenger) writeJoinInvite(conn net.Conn) error {
	buf := &bytes.Buffer{}
	msgr.Debugf("%s: writeJoinInvite", msgr.connId(conn))
	encode(msgr.hostId, buf)
	return msgr.writeMessage(conn, &message{MessageId: newId(), MessageType: joinInvite, Body: buf.Bytes()})
}

func (msgr *messenger) writeJoinAccept(conn net.Conn) error {
	var topics []topic = msgr.getTopics()
	var servers []hostId = msgr.getServerIds()

	buf := &bytes.Buffer{}
	msg := joinAcceptBody{Topics: topics, Peers: servers}
	msgr.Debugf("%s: joinAcceptBody: msg = %+v", msgr.connId(conn), msg)
	encode(msg, buf)
	return msgr.writeMessage(conn, &message{MessageId: newId(), MessageType: joinAccept, Body: buf.Bytes()})
}

func (msgr *messenger) readJoinInvite(conn net.Conn) (hostId, error) {
	joinMsg, err := readMessage(conn)
	if err != nil {
		return "", err
	}

	buf := bytes.NewBuffer(joinMsg.Body)
	var reply hostId
	decode(buf, &reply)
	msgr.Debugf("%s: readJoinInvite: reply = %s", msgr.connId(conn), reply)
	return reply, nil
}

func (msgr *messenger) readJoinAccept(conn net.Conn) (*joinAcceptBody, error) {
	joinMsg, err := readMessage(conn)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(joinMsg.Body)
	reply := &joinAcceptBody{}
	decode(buf, reply)
	msgr.Debugf("%s: readJoinAccept: reply = %+v", msgr.connId(conn), *reply)
	return reply, nil
}

func (msgr *messenger) clientReadLoop(clientId hostId) {
	var conn net.Conn = msgr.getClientConn(clientId)
	if conn == nil {
		msgr.Debugf("%s: Received request from unknown client %s. Ignoring", msgr.hostId, clientId)
	}

	for {
		msgr.testReadMessage(conn)
		msg, err := readMessage(conn)
		if err != nil {
			msgr.removeClient(clientId)
			msgr.Debugf("%s: clientReadLoop removed clientId = %s", msgr.connId(conn), clientId)
			if err.Error() == "EOF" {
				msgr.Debugf("%s: Client %s disconnected.", msgr.hostId, clientId)
			} else {
				msgr.Errorf("%s: Failed to read from client. Disconnecting.", msgr.hostId)
			}

			if conn != nil {
				conn.Close()
			}
			return
		}

		switch msg.MessageType {
		case publish, request:
			go msgr.handleRequest(clientId, msg)
		default:
			panic(fmt.Errorf("Read unknown client message type %s", msg.MessageType))
		}
	}
}

func (msgr *messenger) serverReadLoop(serverId hostId) {
	var conn net.Conn = msgr.getServerConn(serverId)
	for {
		msgr.testReadMessage(conn)
		msg, err := readMessage(conn)
		msgr.Debugf("%s: serverReadLoop received message = %+v; err = %+v", msgr.connId(conn), msg, err)
		if err != nil {
			if err.Error() == "EOF" {
				msgr.Infof("%s: %s has left.", msgr.hostId, serverId)
			} else {
				msgr.Errorf("%s: Failed to read from server (%v). Disconnecting.", msgr.connId(conn), err)
			}

			if conn != nil {
				conn.Close()
			}

			state := msgr.getServerState(serverId)
			if state == serverLeaving {
				msgr.removeServer(serverId)
			} else {
				msgr.setServerState(serverId, serverDisconnected)
				go msgr.dial(serverId)
			}

			return
		}

		switch msg.MessageType {
		case reply, replyPanic:
			go msgr.handleReply(serverId, msg)
		case subscribe, unsubscribe:
			go msgr.handleSubscription(serverId, msg)
		case leaving:
			go msgr.handleLeaving(serverId)
		default:
			panic(fmt.Errorf("Read unknown client message type %s", msg.MessageType))
		}
	}
}

func (msgr *messenger) handleRequest(clientId hostId, msg *message) {
	handler := msgr.getHandler(msg.Topic)
	if handler == nil {
		msgr.Errorf("Received '%s' message for non-subscribed topic %s. Ignored.", msg.MessageType, msg.Topic)
		return
	}

	result, err := msgr.runHandler(string(msg.Topic), msg.Body, handler)
	msgr.Debugf("%s: runHandler returned: result = %s; err = %+v", msgr.hostId, string(result), err)
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

	conn := msgr.getClientConn(clientId)
	if conn == nil {
		msgr.Errorf("Lost connection to %s. Message is ignored.", clientId)
		return
	}
	msgr.writeMessage(conn, reply)
}

func (msgr *messenger) runHandler(topic string, body []byte, handler Handler) (result []byte, err error) {
	defer func() {
		recErr := recover()
		if recErr != nil {
			msgr.Panic(recErr, string(debug.Stack()))
			result = nil
			err = PanicError
		}
	}()

	result = handler(topic, body)
	msgr.Debugf("%s: runHandler: err = %v", msgr.hostId, err)
	return result, err
}

func (msgr *messenger) handleLeaving(serverId hostId) {
	msgr.Debugf("%s: handleRequest: received leaving message", msgr.hostId)
	msgr.setServerState(serverId, serverLeaving)
	// TODO: Graceful shutdown
}

func (msgr *messenger) handleReply(serverId hostId, msg *message) {
	pending := msgr.getReplyChan(serverId, msg.MessageId)
	if pending == nil {
		msgr.Errorf("Received unexpected message '%s'. Ignored.", msg.MessageType)
		return
	}

	msgr.Debugf("%s: handleReply: msg = %+v", msgr.hostId, msg)
	if msg.MessageType == replyPanic {
		pending <- &replyMessage{message: msg, error: PanicError}
	} else {
		pending <- &replyMessage{message: msg}
	}
}

func (msgr *messenger) handleSubscription(serverId hostId, msg *message) {

	buf := bytes.NewBuffer(msg.Body)
	var _topic topic
	decode(buf, &_topic)

	switch msg.MessageType {
	case subscribe:
		msgr.addServerTopic(serverId, _topic)
	case unsubscribe:
		msgr.removeServerTopic(serverId, _topic)
	default:
		panic("Wrong message type for handleSubscription")
	}
}

func (msgr *messenger) Leave() {
	msgr.closing = true
	if msgr.Listener != nil {
		msgr.Listener.Close()
	}
	msgr.clientBroadcast(leaving, nil)
	// TODO: Graceful shutdown
}

func (msgr *messenger) broadcastSubscribtion(msgType messageType, topic string) {
	if msgr.Listener == nil {
		return
	}
	buf := &bytes.Buffer{}
	encode(topic, buf)

	msgr.clientBroadcast(msgType, buf.Bytes())
}

func (msgr *messenger) Publish(_topic string, body []byte) error {
	msg := &message{
		MessageId:   newId(),
		MessageType: publish,
		Topic:       topic(_topic),
		Body:        body,
	}

	for {
		to := msgr.selectTopicServer(topic(_topic))
		if to == "" {
			return NoSubscribersError
		}
		conn := msgr.getServerConn(to)
		if conn == nil {
			msgr.setServerState(to, serverDisconnected)
			continue
		}

		err := msgr.writeMessage(conn, msg)
		if err == nil {
			return nil
		}
	}
}

func (msgr *messenger) Broadcast(_topic string, body []byte) error {
	serverIds := msgr.getServerIdsByTopic(topic(_topic))
	msg := &message{
		MessageId:   newId(),
		MessageType: publish,
		Topic:       topic(_topic),
		Body:        body,
	}

	if len(serverIds) == 0 {
		return NoSubscribersError
	}
	var wg = sync.WaitGroup{}
	wg.Add(len(serverIds))
	for _, to := range serverIds {
		clientId := to
		conn := msgr.getServerConn(clientId)
		go func() {
			if conn != nil {
				msgr.writeMessage(conn, msg)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return nil
}

func (msgr *messenger) clientBroadcast(msgType messageType, body []byte) {
	clientIds := msgr.getClientIds()
	msgr.Debugf("%s: clientBroadcast.10: Broadcasting message %s to clients: %s", msgr.hostId, msgType, clientIds)
	msg := &message{
		MessageId:   newId(),
		MessageType: msgType,
		Topic:       "",
		Body:        body,
	}

	if len(clientIds) == 0 {
		return
	}
	var wg = sync.WaitGroup{}
	wg.Add(len(clientIds))
	for _, to := range clientIds {
		clientId := to
		conn := msgr.getClientConn(clientId)
		go func() {
			if conn != nil {
				msgr.Debugf("%s: clientBroadcast.20: writing message", msgr.connId(conn))
				msgr.writeMessage(conn, msg)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return
}

func (msgr *messenger) Request(_topic string, body []byte, timeout time.Duration) ([]byte, error) {
	timeoutChan := time.After(timeout)

	msg := &message{
		MessageId:   newId(),
		MessageType: request,
		Topic:       topic(_topic),
		Body:        body,
	}

	for {
		serverId := msgr.selectTopicServer(topic(_topic))
		if serverId == "" {
			return []byte{}, NoSubscribersError
		}
		conn := msgr.getServerConn(serverId)
		if conn == nil {
			msgr.setServerState(serverId, serverDisconnected)
			continue
		}

		replyChan := msgr.setReplyChan(serverId, msg.MessageId)
		err := msgr.writeMessage(conn, msg)

		if err == nil {
			select {
			case <-timeoutChan:
				msgr.removeReplyChan(serverId, msg.MessageId)
				return nil, TimeoutError
			case reply := <-replyChan:
				msgr.Debugf("%s: Request: reply = %+v", msgr.hostId, reply)
				if reply.error == ServerDisconnectedError {
					msgr.removeReplyChan(serverId, msg.MessageId)
					continue
				} else {
					msgr.removeReplyChan(serverId, msg.MessageId)
					return reply.message.Body, reply.error
				}
			}

		} else {
			msgr.removeReplyChan(serverId, msg.MessageId)
			continue
		}
	}
}

func (msgr *messenger) selectTopicServer(t topic) hostId {
	serverIds := msgr.getServerIdsByTopic(t)
	if len(serverIds) == 0 {
		return ""
	}
	serverId := serverIds[mRand.Intn(len(serverIds))]
	// msgr.Infof("selectTopicServer: servers[%d] = %s", len(serverIds), serverId)
	return serverId
}

func (msgr *messenger) Survey(topic string, body []byte, timeout time.Duration) ([][]byte, error) {
	return nil, nil
}

func (msgr *messenger) SetLogger(logger Logger) {
	msgr.Logger = logger
}

func (msgr *messenger) connId(conn net.Conn) string {
	if conn == nil {
		return fmt.Sprintf("%s/<nil>", msgr.hostId)
	}
	return fmt.Sprintf("%s/%s->%s", msgr.hostId, conn.LocalAddr(), conn.RemoteAddr())
}

func readMessage(from net.Conn) (*message, error) {
	if from == nil {
		return nil, NilConnError
	}
	lenBuf := make([]byte, 4)
	readBuf := lenBuf

	for len(readBuf) > 0 {
		n, err := from.Read(readBuf)
		if err != nil {
			return nil, err
		}
		readBuf = readBuf[n:]
	}

	msgSize := getUint32(lenBuf)
	msgBytes := make([]byte, msgSize)
	readBuf = msgBytes
	for len(readBuf) > 0 {
		n, err := from.Read(readBuf)
		if err != nil {
			return nil, err
		}
		readBuf = readBuf[n:]
	}
	msgBuf := bytes.NewBuffer(msgBytes)
	msg := &message{}
	decode(msgBuf, msg)
	return msg, nil
}

func (msgr *messenger) writeMessage(to net.Conn, msg *message) error {
	buf := bytes.NewBuffer(make([]byte, 4, 128))
	encode(msg, buf)
	bufSize := buf.Len()
	putUint32(buf.Bytes(), uint32(bufSize-4))
	n, err := to.Write(buf.Bytes())
	if err == nil && n != bufSize {
		return fmt.Errorf("%s: Failed to write to %s.", msgr.hostId, to.RemoteAddr())
	}
	return err
}

func getUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func putUint32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

var ch codec.CborHandle

func encode(v interface{}, buf *bytes.Buffer) {
	codec.NewEncoder(buf, &ch).MustEncode(v)
}

func decode(buf *bytes.Buffer, v interface{}) {
	dec := codec.NewDecoder(buf, &ch)
	dec.MustDecode(v)
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

func (h *server) String() string {
	return fmt.Sprintf("[server: id: %s; topics %d; pendingReplies %d]", h.hostId, len(h.topics), len(h.pendingReplies))
}

func (msg *message) String() string {
	if msg == nil {
		return "[message: nil]"
	}
	return fmt.Sprintf("[message[%s/%s]: topic: %s; body.len: %d]", msg.MessageId, msg.MessageType, msg.Topic, len(msg.Body))
}

func (pr *replyMessage) String() string {
	return fmt.Sprintf("[replyMessage: msg: %s; err = %v]", pr.message, pr.error)
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
