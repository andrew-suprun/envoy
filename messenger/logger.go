package messenger

import "log"

type Logger interface {
	Debug(format string, v ...interface{})
	Info(format string, v ...interface{})
	Error(format string, v ...interface{})
	Panic(err *PanicError)
}

type _defaultLogger struct{}

func (l _defaultLogger) Debug(format string, v ...interface{}) {
	log.Printf("DEBUG: "+format, v...)
}

func (l _defaultLogger) Info(format string, v ...interface{}) {
	log.Printf("INFO:  "+format, v...)
}

func (l _defaultLogger) Error(format string, v ...interface{}) {
	log.Printf("ERROR: "+format, v...)
}

func (l _defaultLogger) Panic(err *PanicError) {

}

var defaultLogger _defaultLogger
