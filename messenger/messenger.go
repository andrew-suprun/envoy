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
	"sync"
	"time"
)

var (
	TimeoutError       = errors.New("timed out")
	NoSubscribersError = errors.New("no subscribers")
	NilConnError       = errors.New("null connection")
)

var ReconnectInterval = 10 * time.Second

type Messenger interface {
	Join(local string, timeout time.Duration, remotes ...string) int
	Leave()

	Request(topic string, body []byte, timeout time.Duration) ([]byte, error)
	Survey(topic string, body []byte, timeout time.Duration) ([][]byte, error)
	Publish(topic string, body []byte) error
	Broadcast(topic string, body []byte) error

	// No more then one subscription per topic.
	// Second subscription panics.
	Subscribe(topic string, handler Handler) error
	Unsubscribe(topic string) error

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
	join
	leave
	subscribe
	unsubscribe
)

const (
	ok replyCode = iota
	disconnected
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
	replyCode      int
)

type messenger struct {
	hostId
	net.Listener
	Logger
	subscriptions
	servers
	clients
	pendingDial
	cond            *sync.Cond
	ticker          *time.Ticker
	testReadMessage func(conn net.Conn)
	closing         bool
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
}

type client struct {
	sync.Mutex
	hostId
	conn net.Conn
}

type pendingDial struct {
	sync.Mutex
	ids map[hostId]struct{}
}

type message struct {
	MessageId   messageId   `codec:"id"`
	MessageType messageType `codec:"mt"`
	Topic       topic       `codec:"t,omitempty"`
	Body        []byte      `codec:"b,omitempty"`
}

type replyMessage struct {
	*message
	replyCode
}

type joinMessageBody struct {
	HostId hostId   `codec:"id,omitempty"`
	Topics []topic  `codec:"t,omitempty"`
	Peers  []hostId `codec:"p,omitempty"`
}

type subscribeMessageBody struct {
	HostId hostId `codec:"id,omitempty"`
	Topic  topic  `codec:"t,omitempty"`
}

func NewMessenger() Messenger {
	return &messenger{
		Logger:          defaultLogger,
		subscriptions:   subscriptions{handlers: make(handlers)},
		clients:         clients{hosts: make(map[hostId]*client)},
		servers:         servers{hosts: make(map[hostId]*server)},
		cond:            sync.NewCond(&sync.Mutex{}),
		ticker:          time.NewTicker(ReconnectInterval),
		pendingDial:     pendingDial{ids: make(map[hostId]struct{})},
		testReadMessage: func(conn net.Conn) {},
	}
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

func (msgr *messenger) Subscribe(_topic string, handler Handler) error {
	msgr.subscriptions.Lock()
	msgr.subscriptions.handlers[topic(_topic)] = handler
	msgr.subscriptions.Unlock()
	return msgr.broadcastSubscribtion(_topic, subscribe)
}

func (msgr *messenger) Unsubscribe(_topic string) error {
	msgr.subscriptions.Lock()
	delete(msgr.subscriptions.handlers, topic(_topic))
	msgr.subscriptions.Unlock()
	return msgr.broadcastSubscribtion(_topic, unsubscribe)
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

func (msgr *messenger) newServer(serverId hostId, conn net.Conn) {
	server := &server{
		hostId:         serverId,
		conn:           conn,
		pendingReplies: make(pendingReplies),
	}

	msgr.servers.Lock()
	msgr.servers.hosts[serverId] = server
	msgr.servers.Unlock()
	msgr.Debugf("%s: newServer: %s", msgr.connId(conn), server)

	go msgr.serverReadLoop(serverId)
}

func (msgr *messenger) setServerTopics(serverId hostId, topics []topic) {
	msgr.servers.Lock()
	server := msgr.servers.hosts[serverId]
	msgr.servers.Unlock()

	if server == nil {
		return
	}

	server.Lock()
	server.topics = mapTopics(topics)
	server.Unlock()
}

func (msgr *messenger) isServerConnected(serverId hostId) bool {
	msgr.servers.Mutex.Lock()
	_, connected := msgr.servers.hosts[serverId]

	msgr.servers.Unlock()
	return connected
}

func (msgr *messenger) getServerIds() []hostId {
	msgr.servers.Mutex.Lock()
	result := make([]hostId, 0, len(msgr.servers.hosts))
	for id := range msgr.servers.hosts {
		result = append(result, id)
	}
	msgr.servers.Unlock()
	return result
}

func (msgr *messenger) getServerIdsByTopic(_topic topic) (result []hostId) {
	serverIds := msgr.getServerIds()
	for _, serverId := range serverIds {
		server := msgr.servers.hosts[serverId]
		if server != nil {
			server.Lock()
			if _, found := server.topics[_topic]; found {
				result = append(result, serverId)
			}
			server.Unlock()
		}
	}
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
	server := msgr.servers.hosts[serverId]
	delete(msgr.servers.hosts, serverId)
	if server != nil {
		for _, replyChan := range server.pendingReplies {
			replyChan <- &replyMessage{replyCode: disconnected}
		}
	}
	msgr.servers.Unlock()
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

func (msgr *messenger) addPendingDial(serverId hostId) {
	msgr.pendingDial.Lock()
	msgr.pendingDial.ids[serverId] = struct{}{}
	msgr.pendingDial.Unlock()
	msgr.cond.L.Lock()
	msgr.cond.Signal()
	msgr.cond.L.Unlock()
}

func (msgr *messenger) removePendingDial(serverId hostId) {
	msgr.pendingDial.Lock()
	delete(msgr.pendingDial.ids, serverId)
	msgr.pendingDial.Unlock()
}

func (msgr *messenger) getPendingDials() []hostId {
	msgr.pendingDial.Lock()
	ids := make([]hostId, 0, len(msgr.pendingDial.ids))
	for id := range msgr.pendingDial.ids {
		ids = append(ids, id)
	}
	msgr.pendingDial.Unlock()
	return ids
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

func (msgr *messenger) Join(local string, timeout time.Duration, remotes ...string) int {
	msgr.Debugf("Join: local = %s; remotes = %s", local, remotes)
	for _, remote := range remotes {
		pendingHost, err := net.ResolveTCPAddr("tcp", remote)
		if err != nil {
			msgr.Errorf("%s: Cannot resolve address %s. Ignoring.", remote)
		}
		msgr.addPendingDial(hostId(pendingHost.String()))
	}

	var err error
	msgr.Listener, err = net.Listen("tcp", local)
	if err != nil {
		msgr.Errorf("Failed to listen on %s", local)
		return 0
	}
	msgr.hostId = hostId(msgr.Listener.Addr().String())
	msgr.Infof("Listening on: %s", msgr.hostId)

	go msgr.acceptConnections()

	msgr.Debugf("%s: Join: pendingDial = %v", msgr.hostId, msgr.getPendingDials())

	dialChan := make(chan int)
	go msgr.dialConnections(dialChan)
	go msgr.tickTock()
	return <-dialChan
}

func (msgr *messenger) tickTock() {
	for {
		<-msgr.ticker.C
		msgr.cond.L.Lock()
		msgr.cond.Signal()
		msgr.cond.L.Unlock()
	}

}

func (msgr *messenger) dialConnections(dialChan chan int) {
	msgr.Debugf("%s: dialConnections: entered", msgr.hostId)
	connections := 0
	sentConnected := false
	for !msgr.closing {
		pending := msgr.getPendingDials()
		msgr.Debugf("%s: dialConnections: pending = %+v", msgr.hostId, pending)

		if !sentConnected {
			clients := msgr.getClientIds()
			msgr.Debugf("%s: dialConnections: clients = %+v", msgr.hostId, clients)
			servers := msgr.getServerIds()
			msgr.Debugf("%s: dialConnections: servers = %+v", msgr.hostId, servers)
			pendingClients := map[hostId]struct{}{}
			for _, hostId := range servers {
				pendingClients[hostId] = struct{}{}
			}
			for _, hostId := range pending {
				pendingClients[hostId] = struct{}{}
			}
			for _, hostId := range clients {
				delete(pendingClients, hostId)
			}
			if len(pendingClients) == 0 {
				sentConnected = true
				dialChan <- connections
				msgr.Debugf("%s: dialConnections. sent %d to dialChan", msgr.hostId, connections)
			}
		}

		if len(pending) == 0 {
			msgr.Debugf("%s: dialConnections. Going to sleep...", msgr.hostId)
			msgr.cond.L.Lock()
			msgr.cond.Wait()
			msgr.cond.L.Unlock()
			msgr.Debugf("%s: dialConnections. Woke up.", msgr.hostId)
		}

		// msgr.Debugf("%s:    dialConnections: pending %s", msgr.hostId, pending)
		for _, _hostId := range pending {
			if msgr.closing {
				break
			}
			if msgr.hostId == _hostId {
				continue
			}

			msgr.Debugf("%s:    dialConnections: connecting to host %s", msgr.hostId, _hostId)

			if msgr.isServerConnected(_hostId) {
				msgr.Debugf("%s:    dialConnections: host %s is connected. Skipping.", msgr.hostId, _hostId)
				msgr.removePendingDial(_hostId)
				continue
			}

			conn, err := net.Dial("tcp", string(_hostId))
			if err != nil {
				msgr.Errorf("Failed to connect to '%s'. Will re-try.", _hostId)
				continue
			}

			err = msgr.writeJoinMessage(conn)
			if err != nil {
				if msgr.closing {
					return
				}
				msgr.Errorf("Failed to write join message to '%s'. Will re-try.", _hostId)
				continue
			}

			msgr.newServer(_hostId, conn)
			msgr.removePendingDial(_hostId)

			connections++
		}
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

		msgr.Debugf("%s: acceptConnections.10: Accepted connection", msgr.connId(conn))
		reply, err := msgr.readJoinMessage(conn)
		msgr.Debugf("%s: acceptConnections.20: Read reply %+v", msgr.connId(conn), reply)
		if err != nil {
			if msgr.closing {
				return
			}
			msgr.Errorf("Failed to read join message from '%s'. Will re-try.", conn)
			continue
		}

		msgr.Debugf("%s: acceptConnections.30: Adding to connect list %+v", msgr.connId(conn), reply.HostId)
		msgr.Debugf("%s: acceptConnections.40: Adding to connect list %+v", msgr.connId(conn), reply.Peers)

		msgr.addPendingDial(reply.HostId)
		for _, hostId := range reply.Peers {
			msgr.addPendingDial(hostId)
		}

		msgr.Debugf("%s: acceptConnections.90: Set topics for server %s %+v", msgr.connId(conn), reply.HostId, reply.Topics)
		msgr.setServerTopics(reply.HostId, reply.Topics)
		msgr.newClient(reply.HostId, conn)
		msgr.Infof("%s: Joined by %s", msgr.hostId, reply.HostId)
	}
}

func (msgr *messenger) writeJoinMessage(conn net.Conn) error {
	var topics []topic = msgr.getTopics()
	var servers []hostId = msgr.getServerIds()

	buf := &bytes.Buffer{}
	msg := joinMessageBody{HostId: msgr.hostId, Topics: topics, Peers: servers}
	codec.NewEncoder(buf, &ch).MustEncode(msg)
	return msgr.writeMessage(conn, &message{MessageId: newId(), MessageType: join, Body: buf.Bytes()})
}

func (msgr *messenger) readJoinMessage(conn net.Conn) (*joinMessageBody, error) {
	joinReplyMsg, err := readMessage(conn)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(joinReplyMsg.Body)
	reply := &joinMessageBody{}
	decode(buf, reply)
	msgr.Debugf("%s: readJoinMessage: reply = %+v", msgr.connId(conn), *reply)
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
			msgr.removeServer(clientId)
			msgr.Debugf("%s: clientReadLoop removed clientId = %s", msgr.connId(conn), clientId)
			if err.Error() == "EOF" {
				msgr.Infof("%s: Client disconnected.", msgr.hostId)
			} else {
				msgr.Errorf("%s: Failed to read from client. Disconnecting.", msgr.hostId)
			}

			conn.Close()
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
	msgr.Debugf("%s: serverReadLoop.1 serverId = %s", msgr.hostId, serverId)
	var conn net.Conn = msgr.getServerConn(serverId)
	msgr.Debugf("%s: serverReadLoop.2 serverId = %s", msgr.connId(conn), serverId)
	for {
		msgr.testReadMessage(conn)
		msg, err := readMessage(conn)
		if err != nil {
			msgr.removeServer(serverId)
			msgr.Debugf("%s: serverReadLoop.3 removed serverId = %s", msgr.connId(conn), serverId)
			if err.Error() == "EOF" {
				msgr.Infof("%s: Server disconnected.", msgr.hostId)
			} else {
				msgr.Errorf("%s: Failed to read from server (%v). Disconnecting.", msgr.connId(conn), err)
			}

			if conn != nil {
				conn.Close()
			}

			msgr.addPendingDial(serverId)
			return
		}

		switch msg.MessageType {
		case reply:
			go msgr.handleReply(serverId, msg)
		case subscribe, unsubscribe:
			go msgr.handleSubscription(serverId, msg)
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

	result := handler(string(msg.Topic), msg.Body)
	if msg.MessageType == publish {
		return
	}
	reply := &message{
		MessageId:   msg.MessageId,
		MessageType: reply,
		Body:        result,
	}
	conn := msgr.getClientConn(clientId)
	if conn == nil {
		msgr.Errorf("Lost connection to %s. Message is ignored.", clientId)
		return
	}
	msgr.writeMessage(conn, reply)
}

func (msgr *messenger) handleReply(serverId hostId, msg *message) {
	pending := msgr.getReplyChan(serverId, msg.MessageId)
	if pending == nil {
		msgr.Errorf("Received unexpected message '%s'. Ignored.", msg.MessageType)
		return
	}

	pending <- &replyMessage{message: msg, replyCode: ok}
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
	// TODO: Graceful shutdown
}

func (msgr *messenger) broadcastSubscribtion(topic string, msgType messageType) error {
	if msgr.Listener == nil {
		return nil
	}
	buf := &bytes.Buffer{}
	encode(topic, buf)

	return msgr.broadcast("", buf.Bytes(), msgType, msgr.getClientIds())
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
			msgr.removeServer(to)
			continue
		}

		return msgr.writeMessage(conn, msg)
	}
}

func (msgr *messenger) Broadcast(_topic string, body []byte) error {
	return msgr.broadcast(_topic, body, publish, msgr.getServerIdsByTopic(topic(_topic)))
}

func (msgr *messenger) broadcast(_topic string, body []byte, msgType messageType, clientIds []hostId) error {
	msg := &message{
		MessageId:   newId(),
		MessageType: msgType,
		Topic:       topic(_topic),
		Body:        body,
	}

	if len(clientIds) == 0 {
		return NoSubscribersError
	}
	var wg = sync.WaitGroup{}
	wg.Add(len(clientIds))
	for _, to := range clientIds {
		clientId := to
		conn := msgr.getClientConn(clientId)
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
			msgr.removeServer(serverId)
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
				switch reply.replyCode {
				case ok:
					msgr.removeReplyChan(serverId, msg.MessageId)
					return reply.message.Body, nil
				case disconnected:
					msgr.removeReplyChan(serverId, msg.MessageId)
					continue
				default:
					panic("unknown replyCode")
				}
			}

		} else {
			msgr.removeReplyChan(serverId, msg.MessageId)
			continue
		}
	}
}

func (msgr *messenger) selectTopicServer(t topic) hostId {
	msgr.Debugf("%s: selectTopicServer.10: topic = %s", msgr.hostId, t)
	serverIds := msgr.getServerIdsByTopic(t)
	msgr.Debugf("%s: selectTopicServer.20: servers = %s", msgr.hostId, serverIds)
	if len(serverIds) == 0 {
		msgr.Debugf("%s: selectTopicServer.30: no servers", msgr.hostId)
		return ""
	}
	serverId := serverIds[mRand.Intn(len(serverIds))]
	msgr.Debugf("%s: selectTopicServer.90: servers = %s", msgr.hostId, serverId)
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
	case join:
		return "join"
	case leave:
		return "leave"
	case subscribe:
		return "subscribe"
	case unsubscribe:
		return "unsubscribe"
	default:
		panic(fmt.Errorf("Unknown messageType %d", mType))
	}
}

func (c replyCode) String() string {
	switch c {
	case ok:
		return "OK"
	case disconnected:
		return "disconnected"
	default:
		return "unknown"
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
	return fmt.Sprintf("[replyMessage: msg: %s; code: %s]", pr.message, pr.replyCode)
}
