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
	Join(local string, remotes []string) error
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

type messenger struct {
	timeout       time.Duration
	retries       int
	local         *net.UDPAddr
	remotes       map[string]*net.UDPAddr
	conn          *net.UDPConn
	subscriptions map[string]func(string, []byte) []byte // key: topic
	subscribers   map[string]map[string]*subscriber      // keys: topic/host
	peers         map[string]*host                       // key: host
	logger        Logger
	*net.UDPConn
	options
	subscriptionsMutex sync.Mutex
	subscribersMutex   sync.Mutex
	peersMutex         sync.Mutex
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

type subscriber struct {
	addr *net.UDPAddr
}

type host struct {
	state               string
	peers               map[string]string
	pendingReplies      map[messageId]*pendingReply
	pendingRepliesMutex sync.Mutex
}

func newHost(peers map[string]string) *host {
	return &host{
		peers:          peers,
		pendingReplies: make(map[messageId]*pendingReply),
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
		remotes:       make(map[string]*net.UDPAddr),
		subscriptions: make(map[string]func(string, []byte) []byte),
		subscribers:   make(map[string]map[string]*subscriber),
		peers:         make(map[string]*host),
		logger:        defaultLogger,
	}
}

func (msgr *messenger) Join(local string, remotes []string) (err error) {
	msgr.local, err = net.ResolveUDPAddr("udp", local)
	if err != nil {
		return err
	}
	local = msgr.local.String()

	delete(msgr.remotes, msgr.local.String())

	msgr.conn, err = net.ListenUDP("udp", msgr.local)
	if err != nil {
		return err
	}
	log.Printf("Listening on: %v", msgr.local)

	go msgr.readLoop()

	msgr.subscriptionsMutex.Lock()
	subs := make([]string, 0, len(msgr.subscriptions))
	for sub := range msgr.subscriptions {
		subs = append(subs, sub)
	}
	msgr.subscriptionsMutex.Unlock()

	buf := &bytes.Buffer{}
	codec.NewEncoder(buf, &ch).MustEncode(joinRequestBody{Topics: subs})
	joinMessageBody := buf.Bytes()

	resultChan := make(chan *message, len(msgr.remotes))
	toJoin := make([]string, len(remotes))
	copy(toJoin, remotes)
	sentInvitations := make(map[string]struct{})
	for len(toJoin) > 0 {
		fmt.Printf("~~~ Join.01: toJoin = %v\n", toJoin)
		invitation := toJoin[len(toJoin)-1]
		raddr, err := net.ResolveUDPAddr("udp", invitation)
		if err != nil {
			msgr.logger.Info("Failed to resolve address %s. Ignoring", invitation)
			continue
		}
		invitation = raddr.String()

		toJoin = toJoin[:len(toJoin)-1]
		sentInvitations[invitation] = struct{}{}

		msgr.remotes[invitation] = raddr

		requestId := newId()
		pendingReplies := map[messageId]*pendingReply{
			requestId: &pendingReply{
				resultChan: resultChan,
			},
		}

		msgr.peersMutex.Lock()
		msgr.peers[invitation] = &host{
			state:          "unknown",
			pendingReplies: pendingReplies,
		}
		msgr.peersMutex.Unlock()

		msgr.sendMessage("", joinMessageBody, requestId, joinRequest, raddr, resultChan)
		fmt.Printf("~~~ Join.02: sent message to = %s\n", invitation)

		select {
		case result := <-resultChan:
			reply := &joinReplyBody{}
			decode(bytes.NewBuffer(result.body), reply)
			from := result.from.String()
			fmt.Printf("~~~ Join.03: received = %+v\n", reply)

			msgr.peers[from] = newHost(reply.Peers)
			for _, peer := range reply.Peers {
				if peer != local {
					_, alreadyPresent := msgr.peers[peer]
					if !alreadyPresent {
						_, alreadySent := sentInvitations[peer]
						if !alreadySent {
							toJoin = append(toJoin, peer)
							fmt.Printf("~~~ Join.04: toJoin = %v\n", toJoin)
						}
					}
				}
			}

			msgr.subscribersMutex.Lock()
			for _, topic := range reply.Topics {
				msgr.addSubscriber(topic, result.from)
			}
			msgr.subscribersMutex.Unlock()

		case <-time.After(msgr.timeout):

		}
		fmt.Printf("~~~ Join.08: toJoin = %v\n", toJoin)
	}

	fmt.Printf("~~~ Join.09: toJoin = %v\n", toJoin)
	return nil
}

func (msgr *messenger) readLoop() {
	byteSlice := make([]byte, bufferSize)
	for {
		n, from, err := msgr.conn.ReadFromUDP(byteSlice) // TODO: Shutdown on closed connection
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

		switch header.MessageType {
		case request:
			continue
		case reply:
			continue
		case joinRequest:
			fmt.Printf("~~~ received joinRequest\n")
			reply := &joinReplyBody{
				Topics: []string{},
				Peers:  make(map[string]string),
			}

			msgr.subscriptionsMutex.Lock()
			for topic := range msgr.subscriptions {
				reply.Topics = append(reply.Topics, topic)
			}
			msgr.subscriptionsMutex.Unlock()

			msgr.peersMutex.Lock()
			for peer, host := range msgr.peers {
				reply.Peers[peer] = host.state
			}
			msgr.peersMutex.Unlock()

			buf := &bytes.Buffer{}
			encode(reply, buf)

			err := msgr.sendMessage("", buf.Bytes(), header.MessageId, joinReply, from, nil)
			if err != nil {
				msgr.logger.Info("Failed to send join reply message to %s: %s", from, err.Error())
				continue
			}

			buf = bytes.NewBuffer(body)
			request := &joinRequestBody{}
			decode(buf, request)

			msgr.subscribersMutex.Lock()
			for _, topic := range request.Topics {
				msgr.addSubscriber(topic, from)
			}
			msgr.subscribersMutex.Unlock()

			continue
		case joinReply:
			fmt.Printf("~~~ readLoop.joinReply.01: from %s\n", from)
			buf := bytes.NewBuffer(body)
			reply := &joinReplyBody{}
			decode(buf, reply)
			fmt.Printf("~~~ readLoop.joinReply.02: reply %+v\n", reply)

			msgr.peersMutex.Lock()
			peer := msgr.peers[from.String()]
			msgr.peersMutex.Unlock()
			fmt.Printf("~~~ readLoop.joinReply.03: peer.pendingReplies %+v\n", peer.pendingReplies)

			peer.pendingRepliesMutex.Lock()
			pr, prFound := peer.pendingReplies[header.MessageId]
			peer.pendingRepliesMutex.Unlock()

			if prFound {
				pr.resultChan <- &message{
					from:   from,
					header: header,
					body:   body,
				}
			} else {
				msgr.logger.Info("There is no message waiting for joinReply from %s", from)
			}

			continue
		default:
			panic(fmt.Errorf("Read unknown message type %d", header.MessageType))
		}

		msgr.peersMutex.Lock()
		host, ok := msgr.peers[from.String()]
		msgr.peersMutex.Unlock()
		if !ok {
			logError(fmt.Errorf("Received message from unknown peer %s. Ignoring.", from.String()))
			continue
		}

		host.pendingRepliesMutex.Lock()
		pending, found := host.pendingReplies[header.MessageId]
		if found {
			delete(host.pendingReplies, header.MessageId)
		}
		host.pendingRepliesMutex.Unlock()

		if found {
			pending.resultChan <- &message{
				from:   from,
				header: header,
				body:   body,
			}
			continue
		}

		msgr.subscriptionsMutex.Lock()
		handler, found := msgr.subscriptions[header.Topic]
		msgr.subscriptionsMutex.Unlock()

		if found {
			go handler(header.Topic, body)
			continue
		}

		logError(fmt.Errorf("Unexpected message %s from %v", header.MessageId, from))
	}
}

func (msgr *messenger) addSubscriber(topic string, addr *net.UDPAddr) {
	peer := addr.String()
	topicMap, ok := msgr.subscribers[topic]
	if !ok {
		topicMap = make(map[string]*subscriber)
		msgr.subscribers[topic] = topicMap
	}
	topicMap[peer] = newSubscriber(addr)
	msgr.logger.Info("Added new subscriber %s/%s", peer, topic)
}

func newSubscriber(addr *net.UDPAddr) *subscriber {
	return &subscriber{addr: addr}
}

type options struct {
	timeout time.Duration
	retries int
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
	msgr.subscriptionsMutex.Lock()
	msgr.subscriptions[topic] = handler
	msgr.subscriptionsMutex.Unlock()

	if msgr.conn != nil {
		// TODO: broadcast subscribe message
	}
	return nil
}

func (msgr *messenger) Unsubscribe(topic string) error {
	msgr.subscriptionsMutex.Lock()
	delete(msgr.subscriptions, topic)
	msgr.subscriptionsMutex.Unlock()

	if msgr.conn != nil {
		// TODO: broadcast unsubscribe message
	}
	return nil
}

func (msgr *messenger) Publish(topic string, body []byte) ([]byte, error) {
	return msgr.publish(topic, body, request)
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
	msgr.logger = logger
}

func (msgr *messenger) selectHost(topic string) (host *net.UDPAddr) {
	msgr.subscribersMutex.Lock()
	defer msgr.subscribersMutex.Unlock()
	topicSubsribers, found := msgr.subscribers[topic]
	if !found {
		return nil
	}

	i := mRand.Intn(len(topicSubsribers))
	for _, subr := range topicSubsribers {
		i--
		if i < 0 {
			return subr.addr
		}
	}

	return nil
}

func (msgr *messenger) publish(topic string, body []byte, msgType messageType) ([]byte, error) {
	to := msgr.selectHost(topic)
	if to == nil {
		return []byte{}, NoSubscribersError
	}

	resultChan := make(chan *message)
	timeoutChan := time.After(msgr.timeout)
	msgr.sendMessage(topic, body, newId(), msgType, to, resultChan)
	select {
	case result := <-resultChan:
		return result.body, nil
	case <-timeoutChan:
		return nil, TimeoutError
	}
}

func newHeader(topic string, msgType messageType) *header {
	return &header{
		MessageId:   newId(),
		MessageType: msgType,
		Topic:       topic,
	}
}

func (msgr *messenger) sendMessage(topic string, body []byte, msgId messageId, msgType messageType, to *net.UDPAddr, resultChan chan *message) error {
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
	_, err := msgr.conn.WriteToUDP(buf.Bytes(), to)
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
