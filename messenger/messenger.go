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
	"time"
)

const (
	DefaultTimeout = 30 * time.Second
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

type messageId [messageIdSize]byte

type messageType int

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
	subscriptions  map[string]Handler // key: topic
	subscribers    map[string]peers   // keys: topic/host
	peers          map[string]*host   // key: host
	pendingReplies map[messageId]*pendingReply
)

type messenger struct {
	*net.UDPAddr
	*net.UDPConn
	Logger
	subscriptions
	subscribers
	peers
	timeout            time.Duration
	retries            int
	subscriptionsMutex sync.Mutex
	subscribersMutex   sync.Mutex
	peersMutex         sync.Mutex
}

func withSubscriptions(msgr *messenger, f func(subscriptions)) {
	msgr.subscriptionsMutex.Lock()
	defer msgr.subscriptionsMutex.Unlock()
	f(msgr.subscriptions)
}

func withSubscribers(msgr *messenger, f func(subscribers)) {
	msgr.subscribersMutex.Lock()
	defer msgr.subscribersMutex.Unlock()
	f(msgr.subscribers)
}

func withPeers(msgr *messenger, f func(peers)) {
	msgr.peersMutex.Lock()
	defer msgr.peersMutex.Unlock()
	f(msgr.peers)
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

type host struct {
	*net.UDPAddr
	state               string
	peers               map[string]string
	pendingReplies      map[messageId]*pendingReply
	pendingRepliesMutex sync.Mutex
}

func withPendingReplies(host *host, f func(pendingReplies)) {
	host.pendingRepliesMutex.Lock()
	defer host.pendingRepliesMutex.Unlock()
	f(host.pendingReplies)
}

func newHost(addr *net.UDPAddr, peers map[string]string) *host {
	return &host{
		UDPAddr:        addr,
		peers:          peers,
		pendingReplies: make(pendingReplies),
	}
}

type message struct {
	from *net.UDPAddr
	*header
	body []byte
}

type pendingReply struct {
	resultChan chan *message
}

////// message bodies

type joinRequestBody struct {
	Topics []string `codec:"subs,omitempty"`
}

type joinReplyBody struct {
	Topics []string          `codec:"subs,omitempty"`
	Peers  map[string]string `codec:"hosts,omitempty"`
}

func newMessenger() Messenger {
	return &messenger{
		timeout:       DefaultTimeout,
		retries:       DefaultRetries,
		Logger:        defaultLogger,
		subscriptions: make(subscriptions),
		subscribers:   make(subscribers),
		peers:         make(peers),
	}
}

func (msgr *messenger) Join(local string, remotes []string) (joined []string, err error) {
	msgr.UDPAddr, err = net.ResolveUDPAddr("udp", local)
	if err != nil {
		return nil, err
	}
	local = msgr.UDPAddr.String()

	msgr.UDPConn, err = net.ListenUDP("udp", msgr.UDPAddr)
	if err != nil {
		return nil, err
	}
	log.Printf("Listening on: %s", local)

	go readLoop(msgr)

	topics := []string{}
	withSubscriptions(msgr, func(subs subscriptions) {
		for topic := range subs {
			topics = append(topics, topic)
		}
	})

	buf := &bytes.Buffer{}
	codec.NewEncoder(buf, &ch).MustEncode(joinRequestBody{Topics: topics})
	joinMessageBody := buf.Bytes()

	resultChan := make(chan *message)
	toJoin := make([]string, len(remotes))
	copy(toJoin, remotes)
	sentInvitations := make(map[string]struct{})
	for len(toJoin) > 0 {
		fmt.Printf("~~~ Join.01: toJoin = %v\n", toJoin)
		invitation := toJoin[len(toJoin)-1]
		raddr, err := net.ResolveUDPAddr("udp", invitation)
		if err != nil {
			msgr.Info("Failed to resolve address %s. Ignoring", invitation)
			continue
		}
		invitation = raddr.String()

		toJoin = toJoin[:len(toJoin)-1]
		sentInvitations[invitation] = struct{}{}

		requestId := newId()
		pendingReplies := pendingReplies{
			requestId: &pendingReply{
				resultChan: resultChan,
			},
		}

		withPeers(msgr, func(peers peers) {
			peers[invitation] = &host{
				state:          "unknown",
				pendingReplies: pendingReplies,
			}
		})

		sendMessage(msgr, "", joinMessageBody, requestId, joinRequest, raddr)
		fmt.Printf("~~~ Join.02: sent message to = %s\n", invitation)

		select {
		case result := <-resultChan:
			reply := &joinReplyBody{}
			decode(bytes.NewBuffer(result.body), reply)
			from := result.from.String()
			fmt.Printf("~~~ Join.03: received = %+v\n", reply)
			joined = append(joined, from)

			fromHost := newHost(nil, reply.Peers)
			withPeers(msgr, func(peers peers) {
				peers[from] = fromHost
			})
			for _, peer := range reply.Peers {
				if peer != local {
					alreadyPresent := false
					withPeers(msgr, func(peers peers) {
						_, alreadyPresent = peers[peer]
					})
					if !alreadyPresent {
						_, alreadySent := sentInvitations[peer]
						if !alreadySent {
							toJoin = append(toJoin, peer)
							fmt.Printf("~~~ Join.04: toJoin = %v\n", toJoin)
						}
					}
				}
			}

			withSubscribers(msgr, func(subsrs subscribers) {
				for _, topic := range reply.Topics {
					addSubscriber(msgr.Logger, subsrs, topic, result.from)
				}
			})

		case <-time.After(msgr.timeout):
			return joined, TimeoutError
		}
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
		fmt.Printf("~~~ received %d bytes from %s; len(byteSlice) = %d\n", n, from, len(byteSlice))
		if n == 0 {
			fmt.Printf("~~~ received empty msg\n")
			continue
		}
		buf := bytes.NewBuffer(byteSlice[:n])
		header := decodeHeader(buf)
		body := buf.Bytes()
		fmt.Printf("~~~ received header %+v; body:'%s'\n", header, string(body))
		msg := &message{from: from, header: header, body: body}

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
	fmt.Printf("~~~ received request\n")
	handler, found := Handler(nil), false
	withSubscriptions(msgr, func(subs subscriptions) {
		handler, found = subs[msg.header.Topic]
	})

	if !found {
		msgr.Info("Received request for non-subscribed topic %s. Ignored.", msg.header.Topic)
		return
	}

	go func() {
		result := handler(msg.header.Topic, msg.body)
		sendMessage(msgr, "", result, msg.header.MessageId, reply, msg.from)
	}()
}

func handleReply(msgr *messenger, msg *message) {
	fmt.Printf("~~~ handleReply.01: msg = %+v\n", msg)
	host, ok := (*host)(nil), false
	withPeers(msgr, func(peers peers) {
		host, ok = peers[msg.from.String()]
	})
	fmt.Printf("~~~ handleReply.02: host = %+v; ok = %v\n", host, ok)
	if !ok {
		logError(fmt.Errorf("Received reply from unknown peer %s. Ignoring.", msg.from.String()))
		return
	}

	pending, found := (*pendingReply)(nil), false
	withPendingReplies(host, func(pendingReplies pendingReplies) {
		pending, found = pendingReplies[msg.header.MessageId]
		if found {
			delete(pendingReplies, msg.header.MessageId)
		}
	})

	if found {
		pending.resultChan <- msg
	}
}

func handleJoinRequest(msgr *messenger, msg *message) {
	fmt.Printf("~~~ received joinRequest\n")
	reply := &joinReplyBody{
		Topics: []string{},
		Peers:  make(map[string]string),
	}

	withSubscriptions(msgr, func(subs subscriptions) {
		for topic := range subs {
			reply.Topics = append(reply.Topics, topic)
		}
	})

	withPeers(msgr, func(peers peers) {
		for peer, host := range peers {
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

	withSubscribers(msgr, func(subsrs subscribers) {
		for _, topic := range request.Topics {
			addSubscriber(msgr.Logger, subsrs, topic, msg.from)
		}
	})

}

func handleJoinReply(msgr *messenger, msg *message) {
	fmt.Printf("~~~ readLoop.joinReply.01: from %s\n", msg.from)
	buf := bytes.NewBuffer(msg.body)
	reply := &joinReplyBody{}
	decode(buf, reply)
	fmt.Printf("~~~ readLoop.joinReply.02: reply %+v\n", reply)

	var peer *host
	withPeers(msgr, func(peers peers) {
		peer = peers[msg.from.String()]
	})
	fmt.Printf("~~~ readLoop.joinReply.03: peer.pendingReplies %+v\n", peer.pendingReplies)

	pr, prFound := (*pendingReply)(nil), false
	withPendingReplies(peer, func(pendingReplies pendingReplies) {
		pr, prFound = pendingReplies[msg.header.MessageId]
	})

	if prFound {
		pr.resultChan <- msg
	} else {
		msgr.Info("There is no message waiting for joinReply from %s", msg.from)
	}
}

func addSubscriber(logger Logger, subsrs subscribers, topic string, addr *net.UDPAddr) {
	peer := addr.String()
	topicMap, ok := subsrs[topic]
	if !ok {
		topicMap = make(map[string]*host)
		subsrs[topic] = topicMap
	}
	topicMap[peer] = newHost(addr, nil)
	logger.Info("Added new subscriber %s/%s", peer, topic)
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}

////// messenger

func (msgr *messenger) Leave() {
}

func (msgr *messenger) Subscribe(topic string, handler Handler) error {
	withSubscriptions(msgr, func(subs subscriptions) {
		subs[topic] = handler
	})

	if msgr.UDPConn != nil {
		// TODO: broadcast subscribe message
	}
	return nil
}

func (msgr *messenger) Unsubscribe(topic string) error {
	withSubscriptions(msgr, func(subs subscriptions) {
		delete(subs, topic)
	})

	if msgr.UDPConn != nil {
		// TODO: broadcast unsubscribe message
	}
	return nil
}

func (msgr *messenger) Publish(topic string, body []byte) ([]byte, error) {
	to := selectHost(msgr, topic)
	if to == nil {
		return []byte{}, NoSubscribersError
	}

	resultChan := make(chan *message)
	timeoutChan := time.After(msgr.timeout)
	msgId := newId()

	withPendingReplies(to, func(p pendingReplies) {
		p[msgId] = &pendingReply{resultChan: resultChan}
	})

	sendMessage(msgr, topic, body, msgId, request, to.UDPAddr)
	select {
	case result := <-resultChan:
		return result.body, nil
	case <-timeoutChan:
		return nil, TimeoutError
	}
}

func selectHost(msgr *messenger, topic string) (host *host) {
	withSubscribers(msgr, func(subsrs subscribers) {
		topicSubsribers, found := subsrs[topic]
		if !found {
			return
		}

		i := mRand.Intn(len(topicSubsribers))
		for _, subsr := range topicSubsribers {
			i--
			if i < 0 {
				host = subsr
				return
			}
		}
		return
	})
	return host
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

	fmt.Printf("~~~ writing to %s %d bytes\n", to, buf.Len())
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
