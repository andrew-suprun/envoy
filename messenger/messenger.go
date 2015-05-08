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

var TimeoutError = errors.New("timed out")
var noSubscribersError = errors.New("no subscribers")

type Messenger interface {
	// No more then one subscription per topic.
	// Second subscription panics.
	Subscribe(topic string, handler func(topic string, body []byte) []byte)
	Unsubscribe(topic string)

	Publish(topic string, body []byte) ([]byte, error)
	Broadcast(topic string, body []byte) []Result

	Join(remotes []string) error
}

type Options struct {
	Timeout *time.Duration
	Retries *int
}

const DefaultTimeout = 30 * time.Second

type Result struct {
	Body  []byte
	Error error
}

func NewMessenger(local string, options *Options) Messenger {
	return &messenger{}
}

func (msgr *messenger) Join(remotes []string) error {
	return msgr.join(remotes)
}

//
// impl
//

const bufferSize = 8*1024 + 8

var c int64

var ch codec.CborHandle

type messenger struct {
	local string
	subscriptions
	subscribers
	hosts
	pendingMessages
	*net.UDPConn
	options
}

type options struct {
	timeout time.Duration
	retries int
}

type subscriptions struct {
	subs map[string]func(string, []byte) []byte // key: topic
	sync.Mutex
}

type subscribers struct {
	subs map[string]map[string]*subscriber // keys: topic/host
	sync.Mutex
}

type subscriber struct {
}

type hosts struct {
	hosts map[string]host // key: host
	sync.Mutex
}

type host struct {
}

type pendingMessages struct {
	messages map[messageId]*pendingMessage
	sync.Mutex
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

func newMessenger(conn *net.UDPConn) *messenger {
	return &messenger{
		UDPConn:       conn,
		subscriptions: subscriptions{subs: make(map[string]func(string, []byte) []byte)},
	}
}

func (msgr *messenger) join(remotes []string) error {
	laddr, err := net.ResolveUDPAddr("udp", msgr.local)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		return err
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
	return nil
}

func (msgr *messenger) Subscribe(topic string, handler func(string, []byte) []byte) {
	msgr.subscribe(topic, handler)
}

func (msgr *messenger) Unsubscribe(topic string) {
	msgr.unsubscribe(topic)
}

func (msgr *messenger) Publish(topic string, body []byte) ([]byte, error) {
	msg := newMessage(topic, body)
	hostName := msgr.selectHost(msg.Topic)
	if hostName == "" {
		return []byte{}, noSubscribersError
	}

	result := <-msgr.sendMessage(hostName, msg)
	return result.Body, result.Error
}

func (msgr *messenger) Broadcast(topic string, body []byte) []Result {
	return []Result{}
}

func (msgr *messenger) SetTimeout(timeout time.Duration) {
	msgr.timeout = timeout
}

func (msgr *messenger) SetRetries(retries int) {
	msgr.retries = retries
}

////// message

func newMessage(topic string, body []byte) *message {
	return &message{
		MessageId: newId(),
		Topic:     topic,
		BodyPart:  body,
	}
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

////// subscriptions

func (subs *subscriptions) subscribe(topic string, handler func(string, []byte) []byte) {
	subs.Lock()
	subs.subs[topic] = handler
	subs.Unlock()
}

func (subs *subscriptions) unsubscribe(topic string) {
	subs.Lock()
	delete(subs.subs, topic)
	subs.Unlock()
}

func (subs *subscriptions) selectHost(topic string) string {
	return ""
}

////// hosts

func (h *hosts) sendMessage(host string, msg *message) chan Result {
	return nil
}

////// misc

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
