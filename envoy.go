package envoy

import (
	"errors"
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
	Log            Logger
)

var NewMessenger func(local string) (Messenger, error)

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
