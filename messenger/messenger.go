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
	"sync/atomic"
	"time"
)

var (
	TimeoutError       = errors.New("timed out")
	NoSubscribersError = errors.New("no subscribers")
	FailedToJoinError  = errors.New("failed to join")
)

type Messenger interface {
	Join(local string, timeout time.Duration, remotes ...string) error
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
	messageIdSize   = 16
	maxBodyPartSize = 8 * 1024
	bufferSize      = maxBodyPartSize + 8
)

type (
	topic          string
	hostId         string
	messageId      [messageIdSize]byte
	messageType    int
	handlers       map[topic]Handler
	hosts          map[hostId]*host
	pendingReplies map[messageId]chan *pendingReply
	replyCode      int
)

type messenger struct {
	hostId
	net.Listener
	Logger
	subscriptions
	peers
	pendingReconnects
	testReadMessage func(conn net.Conn)
	closing         bool
}

type subscriptions struct {
	sync.Mutex
	handlers
}

type pendingReconnects struct {
	sync.Mutex
	hosts map[hostId]struct{}
}

type peers struct {
	sync.Mutex
	hosts
}

type host struct {
	sync.Mutex
	hostId
	net.Addr
	net.Conn
	pendingReplies
	peers  map[hostId]struct{}
	topics map[topic]struct{}
}

type message struct {
	MessageId   messageId   `codec:"id"`
	MessageType messageType `codec:"mt"`
	Topic       string      `codec:"t,omitempty"`
	Body        []byte      `codec:"b,omitempty"`
}

type pendingReply struct {
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
		Logger:            defaultLogger,
		subscriptions:     subscriptions{handlers: make(handlers)},
		peers:             peers{hosts: make(hosts)},
		pendingReconnects: pendingReconnects{hosts: map[hostId]struct{}{}},
		testReadMessage:   func(conn net.Conn) {},
	}
}

func (msgr *messenger) withSubscriptions(f func(handlers)) {
	msgr.subscriptions.Lock()
	defer msgr.subscriptions.Unlock()
	f(msgr.subscriptions.handlers)
}

func (msgr *messenger) withReconnects(f func(map[hostId]struct{})) {
	msgr.pendingReconnects.Lock()
	defer msgr.pendingReconnects.Unlock()
	f(msgr.pendingReconnects.hosts)
}

func (msgr *messenger) withPeers(f func(hosts)) {
	msgr.peers.Mutex.Lock()
	defer msgr.peers.Unlock()
	f(msgr.peers.hosts)
}

func newHost(conn net.Conn) *host {
	return &host{
		hostId:         hostId(conn.RemoteAddr().String()),
		Conn:           conn,
		pendingReplies: make(pendingReplies),
		peers:          map[hostId]struct{}{},
		topics:         map[topic]struct{}{},
	}
}

func withHost(host *host, f func()) {
	host.Lock()
	defer host.Unlock()
	f()
}

func (msgr *messenger) Join(local string, timeout time.Duration, remotes ...string) (err error) {
	msgr.Listener, err = net.Listen("tcp", local)
	if err != nil {
		return err
	}
	msgr.hostId = hostId(msgr.Listener.Addr().String())
	msgr.Infof("Listening on: %s", msgr.hostId)

	go msgr.acceptConnections()
	go msgr.reconnects()
	msgr.joinPeers(remotes, timeout)
	return
}

func (msgr *messenger) joinPeers(remotes []string, timeout time.Duration) int {
	toJoinChan := make(chan hostId, len(remotes))
	sentInvitations := make(map[hostId]struct{})
	var (
		invitationMutex sync.Mutex
		sent            int64
		received        int64
	)

	for _, remote := range remotes {
		addr, err := net.ResolveTCPAddr("tcp", remote)
		if err != nil {
			msgr.Errorf("Failed to resolve '%s'. Ignoring.", remote)
		} else {
			toJoinChan <- hostId(addr.String())
		}
	}

	atomic.AddInt64(&sent, int64(len(remotes)))

	timeoutChan := time.After(timeout)
	resultChan := make(chan *host)

	go func() {
		for invitation := range toJoinChan {
			invitationMutex.Lock()
			sentInvitations[hostId(invitation)] = struct{}{}
			invitationMutex.Unlock()

			conn, err := net.Dial("tcp", string(invitation))
			if err != nil {
				msgr.Errorf("Failed to connect to %s: %v", invitation, err)
				return
			}
			host := msgr.joinPeer(conn)
			if host != nil {
				resultChan <- host
			}
		}
	}()

collectResults:
	for {
		select {
		case result := <-resultChan:
			for peer, state := range result.peers {
				_ = state // TODO: Handle peer state
				if peer != msgr.hostId {
					alreadyPresent := false
					msgr.withPeers(func(hosts hosts) {
						_, alreadyPresent = hosts[peer]
					})
					if !alreadyPresent {
						invitationMutex.Lock()
						_, alreadySent := sentInvitations[peer]
						invitationMutex.Unlock()

						if !alreadySent {
							atomic.AddInt64(&sent, 1)
							toJoinChan <- peer
						}
					}
				}
			}
			s := atomic.LoadInt64(&sent)
			r := atomic.AddInt64(&received, 1)
			if s == r {
				break collectResults
			}
		case <-timeoutChan:
			break collectResults
		}
	}
	close(toJoinChan)
	return int(atomic.LoadInt64(&received))
}

func (msgr *messenger) newJoinMessage() *message {
	var topics []topic
	msgr.withSubscriptions(func(h handlers) {
		topics = make([]topic, 0, len(h))
		for topic := range h {
			topics = append(topics, topic)
		}
	})
	var peers []hostId
	msgr.withPeers(func(hosts hosts) {
		peers = make([]hostId, 0, len(hosts))
		for _, peer := range hosts {
			peers = append(peers, peer.hostId)
		}
	})

	buf := &bytes.Buffer{}
	codec.NewEncoder(buf, &ch).MustEncode(joinMessageBody{HostId: msgr.hostId, Topics: topics, Peers: peers})
	return &message{MessageId: newId(), MessageType: join, Body: buf.Bytes()}
}

func (msgr *messenger) joinPeer(conn net.Conn) *host {
	msgr.Debugf("[%s]: joinPeer %s", msgr.hostId, conn.RemoteAddr())
	host := newHost(conn)
	joinMsg := msgr.newJoinMessage()
	err := writeMessage(host, joinMsg)
	if err != nil {
		return nil
	}
	joinReplyMsg, err := readMessage(conn)
	if err != nil {
		return nil
	}

	buf := bytes.NewBuffer(joinReplyMsg.Body)
	reply := &joinMessageBody{}
	decode(buf, reply)

	host.hostId = reply.HostId
	for _, peer := range reply.Peers {
		host.peers[peer] = struct{}{}

	}
	for _, topic := range reply.Topics {
		host.topics[topic] = struct{}{}
	}

	msgr.Debugf("[%s]: joinPeer: got reply %s", msgr.hostId, host)

	var alreadyExist bool
	msgr.withPeers(func(hosts hosts) {
		_, alreadyExist := hosts[host.hostId]
		if !alreadyExist {
			hosts[host.hostId] = host
		}
	})
	if alreadyExist {
		msgr.Debugf("[%s]: %s already exist", msgr.hostId, host.hostId)
		return nil
	}

	msgr.Debugf("[%s]: joinPeer: entering read loop %s", msgr.hostId, host.hostId)
	go msgr.readLoop(host)

	msgr.Infof("%s joined by %s; topics: %s", msgr.hostId, host.hostId, reply.Topics)
	return host
}

func readMessage(from net.Conn) (*message, error) {
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

func writeMessage(to net.Conn, msg *message) error {
	buf := bytes.NewBuffer(make([]byte, 4, 128))
	encode(msg, buf)
	bufSize := buf.Len()
	putUint32(buf.Bytes(), uint32(bufSize-4))
	n, err := to.Write(buf.Bytes())
	if err == nil && n != bufSize {
		return fmt.Errorf("Failed to write to %s.", to.RemoteAddr())
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

func (msgr *messenger) acceptConnections() {
	for !msgr.closing {
		conn, err := msgr.Listener.Accept()
		if err != nil {
			if msgr.closing {
				break
			}
			msgr.Errorf("Failed to accept connection: %s", err)
		} else {
			msgr.joinPeer(conn)
		}
	}
}

var reconnectInterval = 10 * time.Second

func (msgr *messenger) reconnects() {
	for !msgr.closing {
		time.Sleep(reconnectInterval)
		pending := []hostId{}
		msgr.withReconnects(func(hosts map[hostId]struct{}) {
			for hostId := range hosts {
				pending = append(pending, hostId)
			}
		})
		for _, _hostId := range pending {
			var _host *host
			var exist bool
			msgr.withPeers(func(peers hosts) {
				_host, exist = peers[_hostId]
			})
			if !exist {
				conn, err := net.Dial("tcp", string(_hostId))
				msgr.Debugf("reconnect[%s]: dialed to %s; err = %v", msgr.hostId, conn.RemoteAddr(), err)
				if err != nil {
					continue
				}
				_host = msgr.joinPeer(conn)
			}
			if _host != nil {
				msgr.withReconnects(func(hosts map[hostId]struct{}) {
					delete(hosts, _hostId)
				})
			}
		}
	}
}

func (msgr *messenger) readLoop(host *host) {
	for {
		msgr.testReadMessage(host.Conn)
		msg, err := readMessage(host.Conn)
		if err != nil {
			if err.Error() == "EOF" {
				msgr.Infof("Peer %s disconnected.", host.hostId)
			} else {
				msgr.Errorf("Messenger[%s]: Failed to read from %s: %v. Disconnecting.", msgr.hostId, host.hostId, err)
			}
			msgr.disconnect(host.hostId)
			return
		}

		switch msg.MessageType {
		case publish, request:
			go msgr.handleRequest(host, msg)
		case reply:
			go msgr.handleReply(host, msg)
		case subscribe, unsubscribe:
			go msgr.handleSubscription(host, msg)
		default:
			panic(fmt.Errorf("Read unknown message type %s", msg.MessageType))
		}
	}
}

func (msgr *messenger) disconnect(hostId hostId) {
	h, ok := (*host)(nil), false
	msgr.withPeers(func(hosts hosts) {
		h, ok = hosts[hostId]
		if ok {
			delete(hosts, h.hostId)
		}
	})
	if h == nil {
		return
	}
	withHost(h, func() {
		h.Conn.Close()
		for _, prChan := range h.pendingReplies {
			prChan <- &pendingReply{replyCode: disconnected}
		}
	})
	msgr.reconnect(hostId)
}

func (msgr *messenger) reconnect(_hostId hostId) {
	msgr.withReconnects(func(hosts map[hostId]struct{}) {
		hosts[_hostId] = struct{}{}
	})
}

func (msgr *messenger) handleRequest(host *host, msg *message) {
	handler, found := Handler(nil), false
	msgr.withSubscriptions(func(handlers handlers) {
		handler, found = handlers[topic(msg.Topic)]
	})

	if !found {
		msgr.Infof("Received '%s' message for non-subscribed topic %s. Ignored.", msg.MessageType, msg.Topic)
		return
	}

	result := handler(msg.Topic, msg.Body)
	if msg.MessageType == publish {
		return
	}
	reply := &message{
		MessageId:   msg.MessageId,
		MessageType: reply,
		Body:        result,
	}
	writeMessage(host, reply)
}

func (msgr *messenger) handleReply(host *host, msg *message) {
	pending, found := (chan *pendingReply)(nil), false
	withHost(host, func() {
		pending, found = host.pendingReplies[msg.MessageId]
	})

	if found {
		pending <- &pendingReply{message: msg, replyCode: ok}
	}
}

func (msgr *messenger) handleSubscription(host *host, msg *message) {

	buf := bytes.NewBuffer(msg.Body)
	var _topic topic
	decode(buf, &_topic)

	withHost(host, func() {
		switch msg.MessageType {
		case subscribe:
			host.topics[_topic] = struct{}{}
		case unsubscribe:
			delete(host.topics, _topic)
		default:
			panic("Wrong message type for handleSubscription")
		}
	})
}

func (msgr *messenger) Leave() {
	msgr.closing = true
	if msgr.Listener != nil {
		msgr.Listener.Close()
	}
}

func (msgr *messenger) Subscribe(_topic string, handler Handler) error {
	msgr.withSubscriptions(func(handlers handlers) {
		handlers[topic(_topic)] = handler
	})

	return msgr.broadcastSubscribtion(_topic, subscribe)
}

func (msgr *messenger) Unsubscribe(_topic string) error {
	msgr.withSubscriptions(func(handlers handlers) {
		delete(handlers, topic(_topic))
	})

	return msgr.broadcastSubscribtion(_topic, unsubscribe)
}

func (msgr *messenger) broadcastSubscribtion(topic string, msgType messageType) error {
	if msgr.Listener == nil {
		return nil
	}
	buf := &bytes.Buffer{}
	encode(topic, buf)

	return msgr.broadcast("", buf.Bytes(), msgType, msgr.selectAllHosts())
}

func (msgr *messenger) Publish(_topic string, body []byte) error {
	msg := &message{
		MessageId:   newId(),
		MessageType: publish,
		Topic:       _topic,
		Body:        body,
	}

	for {
		to := msgr.selectTopicHost(topic(_topic))
		if to == nil {
			return NoSubscribersError
		}

		return writeMessage(to, msg)
	}
}

func (msgr *messenger) Broadcast(_topic string, body []byte) error {
	return msgr.broadcast(_topic, body, publish, msgr.selectAllTopicHosts(topic(_topic)))
}

func (msgr *messenger) broadcast(_topic string, body []byte, msgType messageType, peers []*host) error {
	msg := &message{
		MessageId:   newId(),
		MessageType: msgType,
		Topic:       _topic,
		Body:        body,
	}

	if len(peers) == 0 {
		return NoSubscribersError
	}
	var wg = sync.WaitGroup{}
	wg.Add(len(peers))
	for _, to := range peers {
		peer := to
		go func() {
			writeMessage(peer, msg)
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
		Topic:       _topic,
		Body:        body,
	}

	for {
		to := msgr.selectTopicHost(topic(_topic))
		if to == nil {
			return []byte{}, NoSubscribersError
		}

		replyChan := make(chan *pendingReply)
		withHost(to, func() {
			to.pendingReplies[msg.MessageId] = replyChan
		})

		err := writeMessage(to, msg)

		if err == nil {
			select {
			case <-timeoutChan:
				withHost(to, func() {
					delete(to.pendingReplies, msg.MessageId)
				})
				return nil, TimeoutError
			case reply := <-replyChan:
				switch reply.replyCode {
				case ok:
					withHost(to, func() {
						delete(to.pendingReplies, msg.MessageId)
					})
					return reply.message.Body, nil
				case disconnected:
					withHost(to, func() {
						delete(to.pendingReplies, msg.MessageId)
					})
					continue
				default:
					panic("unknown replyCode")
				}
			}

		} else {
			withHost(to, func() {
				delete(to.pendingReplies, msg.MessageId)
			})
			continue
		}
	}
}

func (msgr *messenger) selectTopicHost(t topic) (peer *host) {
	peers := msgr.selectAllTopicHosts(t)
	if len(peers) == 0 {
		return nil
	}
	return peers[mRand.Intn(len(peers))]
}

func (msgr *messenger) selectAllTopicHosts(t topic) (result []*host) {
	msgr.withPeers(func(hosts hosts) {
		peers := ([]*host)(nil)
		for _, host := range hosts {
			if _, ok := host.topics[t]; ok {
				peers = append(peers, host)
			}
		}
		result = peers
	})
	return result
}

func (msgr *messenger) selectAllHosts() (result []*host) {
	msgr.withPeers(func(hosts hosts) {
		peers := ([]*host)(nil)
		for _, host := range hosts {
			peers = append(peers, host)
		}
		result = peers
	})
	return result
}

func (msgr *messenger) Survey(topic string, body []byte, timeout time.Duration) ([][]byte, error) {
	return nil, nil
}

func (msgr *messenger) SetLogger(logger Logger) {
	msgr.Logger = logger
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

func (h *host) String() string {
	return fmt.Sprintf("[host: id: %s; topics %d; peers %d; pendingReplies %d; ]", h.hostId,
		len(h.topics), len(h.peers), len(h.pendingReplies))
}

func (msg *message) String() string {
	if msg == nil {
		return "[message: nil]"
	}
	return fmt.Sprintf("[message[%s/%s]: topic: %s; body.len: %d]", msg.MessageId, msg.MessageType, msg.Topic, len(msg.Body))
}

func (pr *pendingReply) String() string {
	return fmt.Sprintf("[pendingReply: msg: %s; code: %s]", pr.message, pr.replyCode)
}
