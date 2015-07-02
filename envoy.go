package envoy

import (
	"errors"
	"log"
	"time"
)

var (
	ServerDisconnectedError = errors.New("server disconnected")
	TimeoutError            = errors.New("timed out")
	NoSubscribersError      = errors.New("no subscribers found")
	NoHandlerError          = errors.New("no handler for topic found")
	PanicError              = errors.New("server panic-ed")
)

var (
	Timeout        time.Duration = 30 * time.Second
	RedialInterval time.Duration = 10 * time.Second
)

var NewMessenger func(local string) (Messenger, error)

type Messenger interface {
	// No more than one subscription per topic.
	// Second subscription panics.
	Subscribe(topic string, handler Handler)
	Unsubscribe(topic string)

	Join(remotes ...string) error
	Leave()

	Publish(topic string, body []byte) (MessageId, error)
	Request(topic string, body []byte) ([]byte, MessageId, error)
	Broadcast(topic string, body []byte) (MessageId, error)
	Survey(topic string, body []byte) ([][]byte, MessageId, error)
}

type MessageId interface {
	String() string
}

type Handler func(topic string, body []byte, msgId MessageId) []byte

var Log Logger = defaultLogger{}

type Logger interface {
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Panic(err interface{}, stack string)
}

type defaultLogger struct{}

func (l defaultLogger) Debugf(format string, v ...interface{}) {
	log.Printf("DEBUG: "+format, v...)
}

func (l defaultLogger) Infof(format string, v ...interface{}) {
	log.Printf("INFO:  "+format, v...)
}

func (l defaultLogger) Errorf(format string, v ...interface{}) {
	log.Printf("ERROR: "+format, v...)
}

func (l defaultLogger) Panic(err interface{}, stack string) {
	log.Printf("PANIC: %v\nStack:\n%s", err, stack)
}
