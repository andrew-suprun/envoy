package common

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/ugorji/go/codec"
	"net"
)

type Sender interface {
	Send(messageType string, params ...interface{})
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
	ReplyTimeout
	ReplyPanic
	Join
	RequestDial
	Leaving
	Left
	Subscribe
	Unsubscribe
)

const (
	messageIdSize = 16
)

type Message struct {
	Topic       Topic   `codec:"t,omitempty"`
	Body        []byte  `codec:"b,omitempty"`
	MessageId   MsgId   `codec:"id"`
	MessageType MsgType `codec:"mt"`
}

type JoinMessage struct {
	HostId HostId   `codec:"h,omitempty"`
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

var ReadMessage func(net.Conn) (*Message, error) = readMessage

// todo: add timeout handling
func readMessage(from net.Conn) (*Message, error) {
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
	msg := &Message{}
	Decode(msgBuf, msg)
	return msg, nil
}

var WriteMessage func(net.Conn, *Message) error = writeMessage

// todo: add timeout handling
func writeMessage(to net.Conn, msg *Message) error {
	buf := bytes.NewBuffer(make([]byte, 4, 128))
	Encode(msg, buf)
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
