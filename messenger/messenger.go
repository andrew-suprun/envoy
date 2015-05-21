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

type PanicError struct {
	MessageId string
	At        net.Addr
	Stack     []byte
}

func (p *PanicError) Error() string {
	return fmt.Sprintf("Message %s panic-ed at %s", p.MessageId, p.At)
}

func NewMessenger() Messenger {
	return newMessenger()
}

type Messenger interface {
	Join(local string, remotes []string, timeout time.Duration) error // todo local address
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
	pingRequest messageType = iota
	pongReply
	request
	ack
	reply
	joinMessage
	leaveMessage
	subscribeRequest
	subscribeReply
	unsubscribeRequest
	unsubscribeReply
)

const (
	active state = iota
	unresponsive
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
	pendingReplies map[messageId]chan *message
	state          int

	messenger struct {
		net.Addr
		net.Listener
		Logger
		subscriptions
		peers
	}

	peers struct {
		sync.Mutex
		hosts
	}

	host struct {
		sync.Mutex
		hostId
		net.Addr
		net.Conn
		state
		pendingReplies
		peers  map[hostId]state
		topics map[topic]struct{}
	}

	message struct {
		MessageId   messageId   `codec:"id"`
		MessageType messageType `codec:"mt"`
		Topic       string      `codec:"t,omitempty"`
		Body        []byte      `codec:"b,omitempty"`
	}

	joinMessageBody struct {
		Topics map[topic]struct{} `codec:"topics,omitempty"`
		Peers  map[hostId]state   `codec:"peers,omitempty"`
	}

	subscriptions struct {
		sync.Mutex
		handlers
	}
)

func withSubscriptions(msgr *messenger, f func(handlers)) {
	msgr.subscriptions.Lock()
	defer msgr.subscriptions.Unlock()
	f(msgr.subscriptions.handlers)
}

func withPeers(msgr *messenger, f func(hosts)) {
	msgr.peers.Mutex.Lock()
	defer msgr.peers.Unlock()
	f(msgr.peers.hosts)
}

func newHost(conn net.Conn) *host {
	return &host{
		hostId:         hostId(conn.RemoteAddr().String()),
		Conn:           conn,
		pendingReplies: make(pendingReplies),
	}
}

func withHost(host *host, f func()) {
	host.Lock()
	defer host.Unlock()
	f()
}

func newMessenger() Messenger {
	return &messenger{
		Logger:        defaultLogger,
		subscriptions: subscriptions{handlers: make(handlers)},
		peers:         peers{hosts: make(hosts)},
	}
}

func (msgr *messenger) Join(local string, remotes []string, timeout time.Duration) (err error) {
	msgr.Listener, err = net.Listen("tcp", local)
	if err != nil {
		return err
	}
	msgr.Addr = msgr.Listener.Addr()
	msgr.Info("Listening on: %s", local)

	if len(remotes) > 0 {
		n := joinRemotes(msgr, remotes, timeout)

		if len(remotes) > 0 && n == 0 {
			return FailedToJoinError
		}
	}

	go acceptConnections(msgr)

	return
}

func joinRemotes(msgr *messenger, remotes []string, timeout time.Duration) int {
	msgr.Debug("joinRemotes: remotes %s", remotes)
	toJoinChan := make(chan hostId, len(remotes))
	sentInvitations := make(map[hostId]struct{})
	var (
		invitationMutex sync.Mutex
		sent            int64
		received        int64
	)

	for _, remote := range remotes {
		toJoinChan <- hostId(remote)
	}

	atomic.AddInt64(&sent, int64(len(remotes)))

	timeoutChan := time.After(timeout)
	resultChan := make(chan *host)

	go func() {
		for invitation := range toJoinChan {
			msgr.Debug("joinRemotes: invitation '%s'", invitation)

			invitationMutex.Lock()
			sentInvitations[hostId(invitation)] = struct{}{}
			invitationMutex.Unlock()

			conn, err := net.Dial("tcp", string(invitation))
			if err != nil {
				msgr.Error("Failed to connect to %s: %v", invitation, err)
				return
			}
			host := join(msgr, conn)
			if host != nil {
				resultChan <- host
			}
		}
	}()

	localHost := hostId(msgr.Addr.String())
	done := false
	for !done {
		msgr.Debug("joinRemotes: for")
		select {
		case result := <-resultChan:
			msgr.Info("Joined %s", result.hostId)
			for peer, state := range result.peers {
				_ = state // TODO: Handle peer state
				if peer != localHost {
					alreadyPresent := false
					withPeers(msgr, func(hosts hosts) {
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
				done = true
				break
			}
		case <-timeoutChan:
			done = true
			break
		}
	}
	close(toJoinChan)
	return int(atomic.LoadInt64(&received))
}

func newJoinMessage(msgr *messenger) *message {
	topics := map[topic]struct{}{}
	withSubscriptions(msgr, func(h handlers) {
		for topic := range h {
			topics[topic] = struct{}{}
		}
	})
	peers := map[hostId]state{}
	withPeers(msgr, func(hosts hosts) {
		for _, peer := range hosts {
			peers[peer.hostId] = peer.state
		}
	})

	buf := &bytes.Buffer{}
	codec.NewEncoder(buf, &ch).MustEncode(joinMessageBody{Topics: topics, Peers: peers})
	return &message{MessageId: newId(), MessageType: joinMessage, Body: buf.Bytes()}
}

func join(msgr *messenger, conn net.Conn) *host {
	msgr.Debug("join: conn %s", conn.RemoteAddr())
	joinMsg := newJoinMessage(msgr)
	err := writeMessage(msgr, conn, joinMsg)
	if err != nil {
		msgr.Error("Failed to write join request to %s: %v", conn.RemoteAddr(), err)
		return nil
	}
	joinReplyMsg, err := readMessage(msgr, conn)
	if err != nil {
		msgr.Error("Failed to read join reply from %s: %v", conn.RemoteAddr(), err)
		return nil
	}

	host := newHost(conn)

	buf := bytes.NewBuffer(joinReplyMsg.Body)
	reply := &joinMessageBody{}
	decode(buf, reply)

	host.peers = reply.Peers
	host.topics = reply.Topics

	withPeers(msgr, func(hosts hosts) {
		hosts[host.hostId] = host
	})

	go readLoop(msgr, host)

	return host
}

func readMessage(msgr *messenger, from net.Conn) (*message, error) {
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

func writeMessage(msgr *messenger, to net.Conn, msg *message) error {
	buf := bytes.NewBuffer(make([]byte, 4, 128))
	encode(msg, buf)
	bufSize := buf.Len()
	putUint32(buf.Bytes(), uint32(bufSize-4))
	n, err := to.Write(buf.Bytes())
	if err == nil && n != bufSize {
		panic(fmt.Sprintf("writeMessage wrote %d bytes, needed %d bytes", n, bufSize))
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

func acceptConnections(msgr *messenger) {
	for {
		conn, err := msgr.Listener.Accept()
		if err != nil {
			msgr.Error("Failed to accept connection")
		} else {
			join(msgr, conn)
		}
	}
}

func readLoop(msgr *messenger, host *host) {
	for {
		msg, err := readMessage(msgr, host.Conn)
		if err != nil {
			if err.Error() == "EOF" {
				msgr.Info("Peer %s disconnected.", host.hostId)
			} else {
				msgr.Error("Failed to read from %s: %v. Disconnecting.", host.hostId, err)
			}
			host.Conn.Close()
			withPeers(msgr, func(hosts hosts) {
				delete(hosts, host.hostId)
			})
			return
		}

		switch msg.MessageType {
		case request:
			go handleRequest(msgr, host, msg)
		case ack, reply:
			go handleReply(msgr, host, msg)
		default:
			panic(fmt.Errorf("Read unknown message type %s", msg.MessageType))
		}
	}
}

func handleRequest(msgr *messenger, host *host, msg *message) {
	handler, found := Handler(nil), false
	withSubscriptions(msgr, func(handlers handlers) {
		handler, found = handlers[topic(msg.Topic)]
	})

	if !found {
		msgr.Info("Received request for non-subscribed topic %s. Ignored.", msg.Topic)
		return
	}

	result := handler(msg.Topic, msg.Body)
	reply := &message{
		MessageId:   msg.MessageId,
		MessageType: reply,
		Body:        result,
	}
	err := writeMessage(msgr, host.Conn, reply)
	if err != nil {
		msgr.Error("Failed to reply to %s: %v.", host.hostId, err)
	}
}

func handleReply(msgr *messenger, host *host, msg *message) {
	pending, found := (chan *message)(nil), false
	withHost(host, func() {
		pending, found = host.pendingReplies[msg.MessageId]
	})

	if found {
		pending <- msg
	} else {
		msgr.Error("Received unexpected reply[%T]: %s", msg, msg)
	}
}

func (msgr *messenger) Leave() {
	// TODO
}

func (msgr *messenger) Subscribe(_topic string, handler Handler) error {
	withSubscriptions(msgr, func(handlers handlers) {
		handlers[topic(_topic)] = handler
	})

	if msgr.Listener != nil {
		// TODO: broadcast subscribe message
	}
	return nil
}

func (msgr *messenger) Unsubscribe(_topic string) error {
	withSubscriptions(msgr, func(handlers handlers) {
		delete(handlers, topic(_topic))
	})

	if msgr.Listener != nil {
		// TODO: broadcast unsubscribe message
	}
	return nil
}

func (msgr *messenger) Publish(_topic string, body []byte) error {
	to := selectHost(msgr, topic(_topic))
	if to == nil {
		return NoSubscribersError
	}

	msg := &message{
		MessageId:   newId(),
		MessageType: request,
		Topic:       _topic,
		Body:        body,
	}

	return writeMessage(msgr, to.Conn, msg)
}

func (msgr *messenger) Request(_topic string, body []byte, timeout time.Duration) ([]byte, error) {
	to := selectHost(msgr, topic(_topic))
	if to == nil {
		return []byte{}, NoSubscribersError
	}

	msg := &message{
		MessageId:   newId(),
		MessageType: request,
		Topic:       _topic,
		Body:        body,
	}

	replyChan := make(chan *message)
	withHost(to, func() {
		to.pendingReplies[msg.MessageId] = replyChan
	})

	err := writeMessage(msgr, to.Conn, msg)

	reply := &message{}
	if err == nil {
		if err == nil {
			select {
			case <-time.After(timeout):
				err = TimeoutError
			case reply = <-replyChan:
			}
		}
	}

	withHost(to, func() {
		delete(to.pendingReplies, msg.MessageId)
	})
	return reply.Body, err
}

func selectHost(msgr *messenger, t topic) (peer *host) {
	withPeers(msgr, func(hosts hosts) {
		subscribers := ([]*host)(nil)
		unresponsive := ([]*host)(nil)
		for _, host := range hosts {
			if _, ok := host.topics[t]; ok {
				if host.state == active {
					subscribers = append(subscribers, host)
				} else {
					subscribers = append(subscribers, host)
				}
			}
		}
		if len(subscribers) == 0 {
			subscribers = unresponsive
		}
		if len(subscribers) == 0 {
			return
		}

		peer = subscribers[mRand.Intn(len(subscribers))]
		return
	})
	return peer
}

func (msgr *messenger) Broadcast(topic string, body []byte) error {
	return nil
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
	case pingRequest:
		return "pingRequest"
	case pongReply:
		return "pongReply"
	case request:
		return "request"
	case ack:
		return "ack"
	case reply:
		return "reply"
	case joinMessage:
		return "joinMessage"
	case leaveMessage:
		return "leaveMessage"
	case subscribeRequest:
		return "subscribeRequest"
	case subscribeReply:
		return "subscribeReply"
	case unsubscribeRequest:
		return "unsubscribeRequest"
	case unsubscribeReply:
		return "unsubscribeReply"
	default:
		panic(fmt.Errorf("Unknown messageType %d", mType))
	}
}

func (s state) String() string {
	switch s {
	case active:
		return "active"
	case unresponsive:
		return "unresponsive"
	default:
		return "unknown"
	}
}

func (h *host) String() string {
	return fmt.Sprintf("[host: id: %s; state %s; topics %d; peers %d; pendingReplies %d; ]", h.hostId, h.state,
		len(h.topics), len(h.peers), len(h.pendingReplies))
}

func (msg *message) String() string {
	if msg == nil {
		return "[message: nil]"
	}
	return fmt.Sprintf("[message[%s/%s]: topic: %s; body.len: %d]", msg.MessageId, msg.MessageType, msg.Topic, len(msg.Body))
}
