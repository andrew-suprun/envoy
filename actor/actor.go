package actor

import (
	"fmt"
	"sync"
)

type Actor interface {
	RegisterHandler(MessageType, Handler) Actor
	Start() Actor
	Send(MessageType, Payload) Actor
	Stop()
}

type MessageType string

type Payload interface{}

type Handler func(MessageType, Payload)

func NewActor(name string) Actor {
	return &actor{
		name:     name,
		handlers: make(map[MessageType]Handler),
		Cond:     sync.NewCond(&sync.Mutex{}),
	}
}

type actor struct {
	name     string
	handlers map[MessageType]Handler
	pending  []message
	*sync.Cond
	stopped bool
}

type message struct {
	MessageType
	Payload
}

func (a *actor) RegisterHandler(msgType MessageType, handler Handler) Actor {
	a.handlers[msgType] = handler
	return a
}

func (a *actor) Start() Actor {
	a.logf("started\n")
	go a.run()
	return a
}

func (a *actor) run() {
	for {
		a.logf("entered for loop\n")
		a.Cond.L.Lock()

		if len(a.pending) == 0 {
			a.Cond.Wait()
			a.logf("signaled\n")
		}

		if a.stopped {
			a.Cond.L.Unlock()
			return
		}

		a.logf("pending = %d\n", len(a.pending))
		if len(a.pending) == 0 {
			a.Cond.L.Unlock()
			continue
		}

		msg := a.pending[0]
		a.pending = a.pending[1:]

		a.Cond.L.Unlock()

		a.logf("msg = %#v\n", msg)

		h, found := a.handlers[msg.MessageType]
		if !found {
			panic(fmt.Sprintf("Actor %s received unsupported message type: %s", a.name, msg.MessageType))
		}
		a.logf("found handler for %v\n", msg.MessageType)
		h(msg.MessageType, msg.Payload)
		a.logf("%s handled\n", msg.MessageType)
	}
}

func (a *actor) Send(msgType MessageType, info Payload) Actor {
	a.logf("sending %s: %#v\n", msgType, info)
	a.Cond.L.Lock()
	a.pending = append(a.pending, message{msgType, info})
	a.Cond.Signal()
	a.Cond.L.Unlock()
	return a
}

func (a *actor) Stop() {
	var stopHandler Handler
	var found bool
	a.Cond.L.Lock()
	if a.stopped {
		a.Cond.L.Unlock()
		return
	}
	stopHandler, found = a.handlers["stop"]
	a.stopped = true
	a.Cond.L.Unlock()
	if found {
		stopHandler("stop", nil)
	}
}

func (a *actor) logf(format string, params ...interface{}) {
	// log.Printf(">>> %s: Actor: "+format, append([]interface{}{a.name}, params...)...)
}
