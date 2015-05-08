package messenger

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/ugorji/go/codec"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultTimeout = 30 * time.Second

var TimeoutError = errors.New("timed out")
var NoSubscribersError = errors.New("no subscribers")

func Join(local string, remotes []string, options *Options) (Messenger, error) {
	return join(local, remotes, options)
}

type Messenger interface {
	// No more then one subscription per topic.
	// Second subscription panics.
	Subscribe(topic string, handler func(topic string, body []byte) []byte)
	Unsubscribe(topic string)

	Publish(topic string, body []byte) ([]byte, error)
	Broadcast(topic string, body []byte) ([][]byte, error)

	Leave()
}

type Options struct {
	Timeout *time.Duration
	Retries *int
}

//
// impl
//

const bufferSize = 8*1024 + 8

var c int64

var ch codec.CborHandle

type messenger struct {
	local           string
	remotes         []string
	subscriptions   map[string]func(string, []byte) []byte // key: topic
	subscribers     map[string]map[string]*subscriber      // keys: topic/host
	hosts           map[string]*host                       // key: host
	pendingMessages map[messageId]*pendingMessage
	*net.UDPConn
	options
	subscriptionsMutex   sync.Mutex
	subscribersMutex     sync.Mutex
	hostsMutex           sync.Mutex
	pendingMessagesMutex sync.Mutex
}

type options struct {
	timeout time.Duration
	retries int
}

type subscriber struct {
}

type host struct {
}

type pendingMessage struct {
}

type messageId [16]byte

type message struct {
	MessageId   messageId
	MessageType int
	BodyLen     int
	PartLen     int
	PartOffset  int
	Topic       string
	BodyPart    []byte
}

const (
	PostMessage = iota
	ReplyMessage
	SubscribeMessage
	UnsubscribeMessage
)

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}

////// messenger

func join(local string, remotes []string, options *Options) (Messenger, error) {
	msgr := &messenger{
		local:           local,
		remotes:         remotes,
		subscriptions:   make(map[string]func(string, []byte) []byte),
		subscribers:     make(map[string]map[string]*subscriber),
		hosts:           make(map[string]*host),
		pendingMessages: make(map[messageId]*pendingMessage),
	}
	if options != nil {
		if options.Timeout != nil {
			msgr.timeout = *options.Timeout
		}
		if options.Retries != nil {
			msgr.retries = *options.Retries
		}
	}

	msgr.remotes = remotes
	laddr, err := net.ResolveUDPAddr("udp", msgr.local)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		return nil, err
	}
	log.Printf("    Listening on: %v", laddr)
	buf := make([]byte, bufferSize)
	go func() {
		for {
			n, addr, err := conn.ReadFromUDP(buf)
			if err != nil {
				logError(err)
				continue
			}
			msg := parseBuffer(buf)
			fmt.Printf("~~~ received %s:%s\n", msg.Topic, string(msg.BodyPart))

			conn.WriteToUDP(buf, addr)
			count := atomic.AddInt64(&c, 1)
			if err != nil {
				logError(fmt.Errorf("Failed to read from host %s, port %d: %v", addr.IP.String(), addr.Port, err))
				continue
			}
			log.Printf("    Read %d bytes [%d] from host %s, port %d", n, count, addr.IP.String(), addr.Port)
		}
	}()
	return msgr, nil
}

func (msgr *messenger) Leave() {
}

func (msgr *messenger) Subscribe(topic string, handler func(string, []byte) []byte) {
	msgr.subscriptionsMutex.Lock()
	msgr.subscriptions[topic] = handler
	msgr.subscriptionsMutex.Unlock()

	subscribeMsg := &message{}
	_ = subscribeMsg
}

func (msgr *messenger) Unsubscribe(topic string) {
	// TODO: broadcast unsubscribe message
	msgr.subscriptionsMutex.Lock()
	delete(msgr.subscriptions, topic)
	msgr.subscriptionsMutex.Unlock()
}

func (msgr *messenger) Publish(topic string, body []byte) ([]byte, error) {
	return msgr.publish(topic, body, PostMessage)
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

func (msgr *messenger) selectHost(topic string) (hostName string) {
	return ""
}

func (msgr *messenger) publish(topic string, body []byte, messageType int) ([]byte, error) {
	msg := newMessage(topic, body, messageType)
	hostName := msgr.selectHost(msg.Topic)
	if hostName == "" {
		return []byte{}, NoSubscribersError
	}

	resultChan := make(chan []byte)
	timeoutChan := time.After(msgr.timeout)
	msgr.sendMessage(hostName, msg, resultChan)
	select {
	case result := <-resultChan:
		return result, nil
	case <-timeoutChan:
		return nil, TimeoutError
	}
}

func newMessage(topic string, body []byte, messageType int) *message {
	return &message{
		MessageId:   newId(),
		MessageType: messageType,
		Topic:       topic,
		BodyPart:    body,
	}
}

func (msgr *messenger) sendMessage(hostName string, msg *message, resultChan chan []byte) {

}

func (msg *message) encode() []byte {
	buf := bytes.Buffer{}
	enc := codec.NewEncoder(&buf, &ch)
	err := enc.Encode(msg)
	if err != nil {
		panic("Failed to encode message: " + err.Error())
	}
	return buf.Bytes()
}

func parseBuffer(buf []byte) *message {
	msg := &message{}
	dec := codec.NewDecoderBytes(buf, &ch)
	err := dec.Decode(msg)
	logError(err)
	return msg
}

func newId() (buf [16]byte) {
	rand.Read(buf[:])
	return
}
