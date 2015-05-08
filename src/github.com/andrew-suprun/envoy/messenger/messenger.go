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

// TODO: multi-part messages
// TODO: cluster coordination
// TODO: TLS

var Timeout = errors.New("timed out")

func Join(local string, remotes []string) (Messenger, error) {
	return join(local, remotes)
}

type Messenger interface {
	// No more then one subscription per topic.
	// Second subscription (panics? is ignored? returns an error?)
	Subscribe(topic string, handler func(Message))
	Unsubscribe(topic string)

	Publish(topic string, body []byte) ([]byte, error)
	Broadcast(topic string, body []byte)

	SetTimeout(timeout time.Duration)
	SetRetries(retries int)
}

type Message interface {
	Topic() string
	Body() []byte

	Reply([]byte)
	Ack()
}

//
// impl
//

const bufferSize = 8*1024 + 8

var c int64

var ch codec.CborHandle

type messenger struct {
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
	subs map[string]func(Message) // key: topic
	sync.Mutex
}

type subscribers struct {
	subs map[string]map[string]subscriber // keys: topic/host
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
	messages map[messageId]pendingMessage
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

func join(local string, remotes []string) (Messenger, error) {
	laddr, err := net.ResolveUDPAddr("udp", local)
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
	return newMessenger(conn), nil
}

func newMessenger(conn *net.UDPConn) *messenger {
	return &messenger{
		UDPConn:       conn,
		subscriptions: subscriptions{subs: make(map[string]func(Message))},
	}
}

func (msgr *messenger) Subscribe(topic string, handler func(Message)) {
	msgr.subscribe(topic, handler)
}

func (msgr *messenger) Unsubscribe(topic string) {
	msgr.unsubscribe(topic)
}

func (msgr *messenger) Publish(topic string, body []byte) ([]byte, error) {
	msg := message{
		MessageId: newId(),
		Topic:     topic,
		BodyPart:  body,
	}
	buf := bytes.Buffer{}
	enc := codec.NewEncoder(&buf, &ch)
	err := enc.Encode(msg)
	logError(err)
	hostName := msgr.selectHost(msg.Topic)
	if hostName != "" {
		msgr.sendMessage(hostName, buf.Bytes())
	}
	logError(err)
	return []byte{}, nil
}

func (msgr *messenger) Broadcast(topic string, body []byte) {}

func (msgr *messenger) SetTimeout(timeout time.Duration) {
	msgr.timeout = timeout
}

func (msgr *messenger) SetRetries(retries int) {
	msgr.retries = retries
}

//////

func (subs *subscriptions) subscribe(topic string, handler func(Message)) {
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

//////

func (h *hosts) sendMessage(host string, msg []byte) {
}

//////

func parseBuffer(buf []byte) *message {
	msg := &message{}
	dec := codec.NewDecoderBytes(buf, &ch)
	err := dec.Decode(msg)
	logError(err)
	return msg
}

func newId() [16]byte {
	var buf [16]byte
	rand.Read(buf[:])
	return buf
}
