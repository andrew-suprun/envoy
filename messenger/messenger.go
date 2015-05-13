package messenger

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ugorji/go/codec"
	"log"
	"net"
	"sync"
	"time"
)

const (
	DefaultTimeout = 30 * time.Second
	DefaultRetries = 0
)

var TimeoutError = errors.New("timed out")
var NoSubscribersError = errors.New("no subscribers")

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
	Subscribe(topic string, handler Handler)
	Unsubscribe(topic string)

	SetTimeout(timeout time.Duration)
	SetRetries(retries int)
}

type Handler func(topic string, body []byte) []byte

//
// impl
//

const MessageIdSize = 8

type messageId [MessageIdSize]byte

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
	hosts         map[string]*host                       // key: host
	*net.UDPConn
	options
	subscriptionsMutex sync.Mutex
	subscribersMutex   sync.Mutex
	hostsMutex         sync.Mutex
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
}

type host struct {
	pendingReplies      map[messageId]*pendingReply
	pendingRepliesMutex sync.Mutex
}

type message struct {
	from *net.UDPAddr
	*header
	body []byte
}

type pendingReply struct {
	resultChan chan *message
}

func newMessenger() Messenger {
	return &messenger{
		timeout:       DefaultTimeout,
		retries:       DefaultRetries,
		remotes:       make(map[string]*net.UDPAddr),
		subscriptions: make(map[string]func(string, []byte) []byte),
		subscribers:   make(map[string]map[string]*subscriber),
		hosts:         make(map[string]*host),
	}
}

func (msgr *messenger) Join(local string, remotes []string) (err error) {
	msgr.local, err = net.ResolveUDPAddr("udp", local)
	if err != nil {
		return err
	}

	for _, remote := range remotes {
		raddr, err := net.ResolveUDPAddr("udp", remote)
		if err != nil {
			return err
		}
		msgr.remotes[raddr.String()] = raddr
	}

	msgr.conn, err = net.ListenUDP("udp", msgr.local)
	if err != nil {
		return err
	}
	log.Printf("Listening on: %v", msgr.local)

	go msgr.readLoop()

	resultChan := make(chan []byte, len(msgr.remotes))
	for _, remote := range msgr.remotes {
		msgr.sendMessage("", nil, newId(), joinRequest, remote, resultChan)

		select {
		case result := <-resultChan:
			_ = result
		case <-time.After(msgr.timeout):
		}
	}

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
			msgr.sendMessage("", nil, newId(), joinReply, from, nil)
			continue
		case joinReply:
			fmt.Printf("~~~ received joinReply\n")
			continue
		default:
			panic(fmt.Errorf("Read unknown message type %d", header.MessageType))
		}

		msgr.hostsMutex.Lock()
		host, ok := msgr.hosts[from.String()]
		msgr.hostsMutex.Unlock()
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

type options struct {
	timeout time.Duration
	retries int
}

type joinReplyBody struct {
	Topics []string
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}

////// messenger

func (msgr *messenger) Leave() {
}

func (msgr *messenger) Subscribe(topic string, handler Handler) {
	msgr.subscriptionsMutex.Lock()
	msgr.subscriptions[topic] = handler
	msgr.subscriptionsMutex.Unlock()

	if msgr.conn != nil {
		// TODO: broadcast subscribe message
	}
}

func (msgr *messenger) Unsubscribe(topic string) {
	msgr.subscriptionsMutex.Lock()
	delete(msgr.subscriptions, topic)
	msgr.subscriptionsMutex.Unlock()

	if msgr.conn != nil {
		// TODO: broadcast unsubscribe message
	}
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

func (msgr *messenger) selectHost(topic string) (host *net.UDPAddr) {
	return nil
}

func (msgr *messenger) publish(topic string, body []byte, msgType messageType) ([]byte, error) {
	to := msgr.selectHost(topic)
	if to == nil {
		return []byte{}, NoSubscribersError
	}

	resultChan := make(chan []byte)
	timeoutChan := time.After(msgr.timeout)
	msgr.sendMessage(topic, body, newId(), msgType, to, resultChan)
	select {
	case result := <-resultChan:
		return result, nil
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

func (msgr *messenger) sendMessage(topic string, body []byte, msgId messageId, msgType messageType, to *net.UDPAddr, resultChan chan []byte) {
	msgHeader := header{
		Topic:       topic,
		MessageId:   msgId,
		MessageType: msgType,
		LastPart:    true,
	}

	buf := bytes.Buffer{}
	enc := codec.NewEncoder(&buf, &ch)
	err := enc.Encode(msgHeader)
	logError(err)
	buf.Write(body)

	fmt.Printf("writing to %s %d bytes\n", to, buf.Len())
	_, err = msgr.conn.WriteToUDP(buf.Bytes(), to)
	logError(err)
}

var ch codec.CborHandle

func encodeHeader(h *header, buf *bytes.Buffer) {
	codec.NewEncoder(buf, &ch).MustEncode(h)
}

func decodeHeader(buf *bytes.Buffer) *header {
	msgHeader := &header{}
	dec := codec.NewDecoder(buf, &ch)
	dec.MustDecode(msgHeader)
	return msgHeader
}

func newId() (buf [MessageIdSize]byte) {
	rand.Read(buf[:])
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
