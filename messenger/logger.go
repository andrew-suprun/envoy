package messenger

import "log"

type Logger interface {
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Errorf(format string, v ...interface{})
}

type _defaultLogger struct{}

func (l _defaultLogger) Debugf(format string, v ...interface{}) {
	log.Printf("DEBUG: "+format, v...)
}

func (l _defaultLogger) Infof(format string, v ...interface{}) {
	log.Printf("INFO:  "+format, v...)
}

func (l _defaultLogger) Errorf(format string, v ...interface{}) {
	log.Printf("ERROR: "+format, v...)
}

var defaultLogger _defaultLogger
