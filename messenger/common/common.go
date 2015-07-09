package common

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/andrew-suprun/envoy/future"
	"github.com/ugorji/go/codec"
	"time"
)

var (
	ServerDisconnectedError = errors.New("server disconnected")
	TimeoutError            = errors.New("timed out")
	NoSubscribersError      = errors.New("no subscribers found")
	NoHandlerError          = errors.New("no handler for topic found")
	NilConnError            = errors.New("null connection")
	PanicError              = errors.New("server panic-ed")
)

var (
	Timeout        time.Duration = 30 * time.Second
	RedialInterval time.Duration = 10 * time.Second
	Log            Logger        = &defaultLogger{}
)

type Messenger interface {
	Join(remotes ...string)
	Leave()

	Publish(topic string, body []byte) (MessageId, error)
	Request(topic string, body []byte) ([]byte, MessageId, error)
	Broadcast(topic string, body []byte) (MessageId, error)
	Survey(topic string, body []byte) ([][]byte, MessageId, error)

	// No more than one subscription per topic.
	// Second subscription panics.
	Subscribe(topic string, handler Handler)
	Unsubscribe(topic string)
}

type MessageId interface {
	String() string
}

type Handler func(topic string, body []byte, msgId MessageId) []byte

type Logger interface {
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Panic(err interface{}, stack string)
}
type (
	Topic   string
	HostId  string
	MsgId   [messageIdSize]byte
	MsgType int
)

const (
	Publish MsgType = iota
	Request
	Reply
	Broadcast
	Survey
	ReplyTimeout
	ReplyPanic
	Join
	Leaving
	Subscribe
	Unsubscribe
)

const (
	messageIdSize = 16
)

type (
	MsgNetworkError struct {
		HostId HostId
		Err    error
	}
	MsgAddHost struct{ HostId }
	MsgLeave   struct{ Result future.Future }
)

type Message struct {
	Topic       Topic   `codec:"t,omitempty"`
	Body        []byte  `codec:"b,omitempty"`
	MessageId   MsgId   `codec:"id"`
	MessageType MsgType `codec:"mt"`
}

type JoinMessage struct {
	Topics []Topic  `codec:"t,omitempty"`
	Peers  []HostId `codec:"p,omitempty"`
}

func NewId() (mId MsgId) {
	rand.Read(mId[:])
	return
}

func NewMessage(topic Topic, body []byte, messageType MsgType) *Message {
	return &Message{
		Topic:       topic,
		Body:        body,
		MessageId:   NewId(),
		MessageType: messageType,
	}
}

func (mId MsgId) String() string {
	return hex.EncodeToString(mId[:])
}

func (mType MsgType) String() string {
	switch mType {
	case Publish:
		return "publish"
	case Request:
		return "request"
	case Reply:
		return "reply"
	case Broadcast:
		return "broadcast"
	case Survey:
		return "survey"
	case ReplyTimeout:
		return "replyTimeout"
	case ReplyPanic:
		return "replyPanic"
	case Join:
		return "join"
	case Leaving:
		return "leaving"
	case Subscribe:
		return "subscribe"
	case Unsubscribe:
		return "unsubscribe"
	default:
		panic(fmt.Errorf("Unknown messageType %d", mType))
	}
}

func (msg *Message) String() string {
	if msg == nil {
		return "[message: <nil>]"
	}
	if msg.Body != nil {
		return fmt.Sprintf("[message[%s/%s]: topic: %s; body.len: %d]", msg.MessageId, msg.MessageType, msg.Topic, len(msg.Body))
	}
	return fmt.Sprintf("[message[%s/%s]: topic: %s; body: <nil>]", msg.MessageId, msg.MessageType, msg.Topic)
}

var ch codec.CborHandle

func Encode(v interface{}, buf *bytes.Buffer) {
	codec.NewEncoder(buf, &ch).MustEncode(v)
}

func Decode(buf *bytes.Buffer, v interface{}) {
	dec := codec.NewDecoder(buf, &ch)
	dec.MustDecode(v)
}

func GetUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func PutUint32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}
