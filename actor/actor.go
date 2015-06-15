package actor

import (
	"fmt"
	"sync"
)

type Actor interface {
	RegisterHandler(messageType string, handler Handler) Actor
	Send(messageType string, params ...interface{})
}

type Handler func(messageType string, params []interface{})

func NewActor(name string) Actor {
	return &actor{
		name:     name,
		handlers: make(map[string]Handler),
		Cond:     sync.NewCond(&sync.Mutex{}),
	}
}

type actor struct {
	name     string
	handlers map[string]Handler
	pending  []message
	*sync.Cond
}

type message struct {
	messageType string
	params      []interface{}
}

func (a *actor) RegisterHandler(msgType string, handler Handler) Actor {
	a.handlers[msgType] = handler
	return a
}

func (a *actor) run() {
	a.logf("started")
	for {
		a.Cond.L.Lock()

		if len(a.pending) == 0 {
			a.Cond.Wait()
			a.Cond.L.Unlock()
			continue
		}

		msg := a.pending[0]
		a.pending = a.pending[1:]

		a.Cond.L.Unlock()

		h, found := a.handlers[msg.messageType]
		if !found {
			panic(fmt.Sprintf("Actor %s received unsupported message type: %s", a.name, msg.messageType))
		}
		a.logf("got '%s'", msg.messageType)
		h(msg.messageType, msg.params)
		if msg.messageType == "stop" {
			a.logf("stopped\n")
			return
		}
	}
}

func (a *actor) Send(msgType string, info ...interface{}) {
	a.Cond.L.Lock()
	if a.pending == nil {
		go a.run()
	}
	a.pending = append(a.pending, message{msgType, info})
	a.Cond.Signal()
	a.Cond.L.Unlock()
}

func (a *actor) logf(format string, params ...interface{}) {
	// log.Printf(">>> %s: Actor: "+format, append([]interface{}{a.name}, params...)...)
}
