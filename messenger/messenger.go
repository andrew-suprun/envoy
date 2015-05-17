package messenger

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ugorji/go/codec"
	"log"
	mRand "math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultTimeout = 1 * time.Second
	DefaultRetries = 0
)

var (
	TimeoutError       = errors.New("timed out")
	NoSubscribersError = errors.New("no subscribers")
)

type PanicError struct {
	MessageId string
	At        *net.UDPAddr
	Stack     []byte
}

func (p *PanicError) Error() string {
	return fmt.Sprintf("Message %s panic-ed at %s", p.MessageId, p.At)
}

func NewMessenger() Messenger {
	return newMessenger()
}

type Messenger interface {
	Join(local string, remotes []string) (joined []string, err error)
	Leave()

	Publish(topic string, body []byte) ([]byte, error)
	Broadcast(topic string, body []byte) ([][]byte, error)

	// No more then one subscription per topic.
	// Second subscription panics.
	Subscribe(topic string, handler Handler) error
	Unsubscribe(topic string) error

	SetTimeout(timeout time.Duration)
	SetRetries(retries int)
	SetLogger(logger Logger)
}

type Handler func(topic string, body []byte) []byte

//
// impl
//

const messageIdSize = 16

const (
	pingRequest messageType = iota
	pongReply
	request
	reply
	joinRequest
	joinReply
	leaveMessage
	subscribeRequest
	subscribeReply
	unsubscribeRequest
	unsubscribeReply
)

const (
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
	pendingReplies map[messageId]*pendingReply
)

type messenger struct {
	*net.UDPAddr
	*net.UDPConn
	Logger
	subscriptions
	peers
	timeout time.Duration
	retries int
}

type subscriptions struct {
	sync.Mutex
	handlers
}

func withSubscriptions(msgr *messenger, f func(handlers)) {
	msgr.subscriptions.Lock()
	defer msgr.subscriptions.Unlock()
	f(msgr.subscriptions.handlers)
}

type peers struct {
	sync.Mutex
	hosts
}

func withPeers(msgr *messenger, f func(hosts)) {
	msgr.peers.Mutex.Lock()
	defer msgr.peers.Unlock()
	f(msgr.peers.hosts)
}

type host struct {
	sync.Mutex
	*net.UDPAddr
	state          string
	peers          map[hostId]string
	topics         map[topic]struct{}
	pendingReplies map[messageId]*pendingReply
}

func newHost(addr *net.UDPAddr, peers map[hostId]string, topics map[topic]struct{}) *host {
	return &host{
		UDPAddr:        addr,
		peers:          peers,
		topics:         topics,
		pendingReplies: make(pendingReplies),
	}
}

func withHost(host *host, f func()) {
	host.Lock()
	defer host.Unlock()
	f()
}

type message struct {
	hostId
	from *net.UDPAddr
	*header
	body []byte
}

type header struct {
	MessageId   messageId   `codec:"id"`
	MessageType messageType `codec:"mt"`
	Topic       string      `codec:"t,omitempty"`
	BodyLen     int         `codec:"bl,omitempty"`
	PartIndex   int         `codec:"pi,omitempty"`
	PartOffset  int         `codec:"po,omitempty"`
	LastPart    bool        `codec:"lp,omitempty"`
}

type pendingReply struct {
	resultChan chan *message
}

type joinRequestBody struct {
	Topics map[topic]struct{} `codec:"subs,omitempty"`
}

type joinReplyBody struct {
	Topics map[topic]struct{} `codec:"subs,omitempty"`
	Peers  map[hostId]string  `codec:"hosts,omitempty"`
}

func newMessenger() Messenger {
	return &messenger{
		timeout:       DefaultTimeout,
		retries:       DefaultRetries,
		Logger:        defaultLogger,
		subscriptions: subscriptions{handlers: make(handlers)},
		peers:         peers{hosts: make(hosts)},
	}
}

func (msgr *messenger) Join(local string, remotes []string) (joined []string, err error) {
	msgr.UDPAddr, err = net.ResolveUDPAddr("udp", local)
	if err != nil {
		return nil, err
	}
	localHost := hostId(msgr.UDPAddr.String())

	msgr.UDPConn, err = net.ListenUDP("udp", msgr.UDPAddr)
	if err != nil {
		return nil, err
	}
	log.Printf("Listening on: %s", local)

	go readLoop(msgr)

	if len(remotes) == 0 {
		return
	}

	topics := map[topic]struct{}{}
	withSubscriptions(msgr, func(h handlers) {
		for topic := range h {
			topics[topic] = struct{}{}
		}
	})

	buf := &bytes.Buffer{}
	codec.NewEncoder(buf, &ch).MustEncode(joinRequestBody{Topics: topics})
	joinMessageBody := buf.Bytes()

	timeoutChan := time.After(msgr.timeout)
	resultChan := make(chan *message)
	toJoinChan := make(chan hostId, len(remotes)+8)
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

	go func() {
		for {
			log.Printf("~~~ for")
			select {
			case result := <-resultChan:
				log.Printf("~~~ result: %+v", result)
				reply := &joinReplyBody{}
				decode(bytes.NewBuffer(result.body), reply)
				joined = append(joined, string(result.hostId))

				fromHost := newHost(result.from, reply.Peers, reply.Topics)
				withPeers(msgr, func(hosts hosts) {
					hosts[result.hostId] = fromHost
				})
				msgr.Info("Joined %s", result.hostId)
				for peer, state := range reply.Peers {
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
								log.Printf("~~~ Join: to join: %s", peer)
								atomic.AddInt64(&sent, 1)
								toJoinChan <- peer
							}
						}
					}
				}
				s := atomic.LoadInt64(&sent)
				r := atomic.AddInt64(&received, 1)
				if s == r {
					log.Printf("~~~ break")
					close(toJoinChan)
					return
				}
			case <-timeoutChan:
				log.Printf("~~~ timeout")
				err = TimeoutError
				close(toJoinChan)
				return
			}
		}
	}()

	for invitation := range toJoinChan {
		log.Printf("~~~ invitation = %s", invitation)
		log.Printf("~~~ toJoinChan = %d", len(toJoinChan))
		raddr, err := net.ResolveUDPAddr("udp", string(invitation))
		if err != nil {
			msgr.Info("Failed to resolve address %s (%v). Ignoring.", invitation, err)
			continue
		}
		invitation = hostId(raddr.String())

		invitationMutex.Lock()
		sentInvitations[hostId(invitation)] = struct{}{}
		invitationMutex.Unlock()

		requestId := newId()
		pendingReplies := pendingReplies{
			requestId: &pendingReply{
				resultChan: resultChan,
			},
		}

		withPeers(msgr, func(hosts hosts) {
			hosts[hostId(invitation)] = &host{
				UDPAddr:        raddr,
				state:          "unknown",
				pendingReplies: pendingReplies,
			}
		})

		log.Printf("~~~ inviting %s", invitation)
		sendMessage(msgr, "", joinMessageBody, requestId, joinRequest, raddr)
	}

	return
}

func readLoop(msgr *messenger) {
	byteSlice := make([]byte, bufferSize)
	for {
		n, from, err := msgr.ReadFromUDP(byteSlice) // TODO: Shutdown on closed connection
		if err != nil {
			logError(err)
			continue
		}
		if n == 0 {
			continue
		}
		buf := bytes.NewBuffer(byteSlice[:n])
		header := decodeHeader(buf)
		body := make([]byte, buf.Len())
		copy(body, buf.Bytes())

		msg := &message{hostId: hostId(from.String()), from: from, header: header, body: body}

		switch header.MessageType {
		case request:
			handleRequest(msgr, msg)
		case reply:
			handleReply(msgr, msg)
		case joinRequest:
			handleJoinRequest(msgr, msg)
		case joinReply:
			handleJoinReply(msgr, msg)
		default:
			panic(fmt.Errorf("Read unknown message type %d", header.MessageType))
		}
	}
}

func handleRequest(msgr *messenger, msg *message) {
	handler, found := Handler(nil), false
	withSubscriptions(msgr, func(handlers handlers) {
		handler, found = handlers[topic(msg.header.Topic)]
	})

	if !found {
		msgr.Info("Received request for non-subscribed topic %s. Ignored.", msg.header.Topic)
		return
	}

	go func() {
		result := handler(msg.header.Topic, msg.body)
		sendMessage(msgr, msg.header.Topic, result, msg.header.MessageId, reply, msg.from)
	}()
}

func handleReply(msgr *messenger, msg *message) {
	host, ok := (*host)(nil), false
	withPeers(msgr, func(hosts hosts) {
		host, ok = hosts[msg.hostId]
	})
	if !ok {
		logError(fmt.Errorf("Received reply from unknown peer %s. Ignoring.", msg.hostId))
		return
	}

	pending, found := (*pendingReply)(nil), false
	withHost(host, func() {
		pending, found = host.pendingReplies[msg.header.MessageId]
		if found {
			delete(host.pendingReplies, msg.header.MessageId)
		}
	})

	if found {
		pending.resultChan <- msg
	}
}

func handleJoinRequest(msgr *messenger, msg *message) {
	reply := &joinReplyBody{
		Topics: map[topic]struct{}{},
		Peers:  make(map[hostId]string),
	}

	withSubscriptions(msgr, func(handlers handlers) {
		for topic := range handlers {
			reply.Topics[topic] = struct{}{}
		}
	})

	withPeers(msgr, func(hosts hosts) {
		for peer, host := range hosts {
			reply.Peers[peer] = host.state
		}
	})

	buf := &bytes.Buffer{}
	encode(reply, buf)

	err := sendMessage(msgr, "", buf.Bytes(), msg.header.MessageId, joinReply, msg.from)
	if err != nil {
		msgr.Info("Failed to send join reply message to %s: %s", msg.from, err.Error())
		return
	}

	buf = bytes.NewBuffer(msg.body)
	request := &joinRequestBody{}
	decode(buf, request)

	host := newHost(msg.from, make(map[hostId]string), request.Topics)

	withPeers(msgr, func(hosts hosts) {
		hosts[hostId(msg.hostId)] = host
	})
	msgr.Info("Joined %s", msg.hostId)
}

func handleJoinReply(msgr *messenger, msg *message) {
	buf := bytes.NewBuffer(msg.body)
	reply := &joinReplyBody{}
	decode(buf, reply)

	var peer *host
	withPeers(msgr, func(hosts hosts) {
		peer = hosts[hostId(msg.hostId)]
	})

	pr, prFound := (*pendingReply)(nil), false
	withHost(peer, func() {
		pr, prFound = peer.pendingReplies[msg.header.MessageId]
	})

	if prFound {
		pr.resultChan <- msg
	} else {
		msgr.Info("There is no message waiting for joinReply from %s", msg.from)
	}
}

// func addSubscriber(logger Logger, subsrs subscribers, topic string, addr *net.UDPAddr) {
// 	peer := addr.String()
// 	topicMap, ok := subsrs[topic]
// 	if !ok {
// 		topicMap = make(map[string]*host)
// 		subsrs[topic] = topicMap
// 	}
// 	topicMap[peer] = newHost(addr, nil)
// 	logger.Info("Added new subscriber %s/%s", peer, topic)
// }

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}

func (msgr *messenger) Leave() {
	// TODO
}

func (msgr *messenger) Subscribe(_topic string, handler Handler) error {
	withSubscriptions(msgr, func(handlers handlers) {
		handlers[topic(_topic)] = handler
	})

	if msgr.UDPConn != nil {
		// TODO: broadcast subscribe message
	}
	return nil
}

func (msgr *messenger) Unsubscribe(_topic string) error {
	withSubscriptions(msgr, func(handlers handlers) {
		delete(handlers, topic(_topic))
	})

	if msgr.UDPConn != nil {
		// TODO: broadcast unsubscribe message
	}
	return nil
}

func (msgr *messenger) Publish(_topic string, body []byte) ([]byte, error) {
	to := selectHost(msgr, topic(_topic))
	if to == nil {
		return []byte{}, NoSubscribersError
	}

	resultChan := make(chan *message)
	timeoutChan := time.After(msgr.timeout)
	msgId := newId()

	withHost(to, func() {
		to.pendingReplies[msgId] = &pendingReply{resultChan: resultChan}
	})

	sendMessage(msgr, _topic, body, msgId, request, to.UDPAddr)
	select {
	case result := <-resultChan:
		return result.body, nil
	case <-timeoutChan:
		return nil, TimeoutError
	}
}

func selectHost(msgr *messenger, t topic) (peer *host) {
	withPeers(msgr, func(hosts hosts) {
		subscribers := ([]*host)(nil)
		for _, host := range hosts {
			if _, ok := host.topics[t]; ok {
				subscribers = append(subscribers, host)
			}
		}
		if len(subscribers) == 0 {
			return
		}

		peer = subscribers[mRand.Intn(len(subscribers))]
		return
	})
	return peer
}

func (msgr *messenger) Broadcast(topic string, body []byte) ([][]byte, error) {
	return nil, nil
}

func (msgr *messenger) SetTimeout(timeout time.Duration) {
	msgr.timeout = timeout
}

func (msgr *messenger) SetRetries(retries int) {
	msgr.retries = retries
}

func (msgr *messenger) SetLogger(logger Logger) {
	msgr.Logger = logger
}

func newHeader(topic string, msgType messageType) *header {
	return &header{
		MessageId:   newId(),
		MessageType: msgType,
		Topic:       topic,
	}
}

func sendMessage(msgr *messenger, topic string, body []byte, msgId messageId, msgType messageType, to *net.UDPAddr) error {
	msgHeader := header{
		Topic:       topic,
		MessageId:   msgId,
		MessageType: msgType,
		LastPart:    true,
	}

	buf := bytes.Buffer{}
	enc := codec.NewEncoder(&buf, &ch)
	enc.MustEncode(msgHeader)
	buf.Write(body)

	_, err := msgr.WriteToUDP(buf.Bytes(), to)
	return err
}

var ch codec.CborHandle

func encode(v interface{}, buf *bytes.Buffer) {
	codec.NewEncoder(buf, &ch).MustEncode(v)
}

func decode(buf *bytes.Buffer, v interface{}) {
	dec := codec.NewDecoder(buf, &ch)
	dec.MustDecode(v)
}

func decodeHeader(buf *bytes.Buffer) *header {
	msgHeader := &header{}
	decode(buf, msgHeader)
	return msgHeader
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
	case reply:
		return "reply"
	case joinRequest:
		return "joinRequest"
	case joinReply:
		return "joinReply"
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

func (msg *message) String() string {
	return fmt.Sprintf("[message: from: %s; header: %s; body %s]", msg.from, msg.header, string(msg.body))
}

func (h *header) String() string {
	return fmt.Sprintf("[header: messageId: '%s'; messageType '%s'; topic: '%s']", h.MessageId, h.MessageType, h.Topic)
}
