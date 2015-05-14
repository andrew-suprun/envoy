package messenger

import "log"

type Logger interface {
	Info(format string, v ...interface{})
	Panic(err *PanicError)
}

type _defaultLogger struct{}

func (l _defaultLogger) Info(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (l _defaultLogger) Panic(err *PanicError) {

}

var defaultLogger _defaultLogger
