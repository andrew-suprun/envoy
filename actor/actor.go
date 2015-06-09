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
	go a.run()
	return a
}

func (a *actor) run() {
	for {
		a.Cond.L.Lock()
		a.Cond.Wait()

		if a.stopped {
			a.Cond.L.Unlock()
			return
		}

		if len(a.pending) == 0 {
			a.Cond.L.Unlock()
			continue
		}

		msg := a.pending[0]
		a.pending = a.pending[1:]

		a.Cond.L.Unlock()

		h, found := a.handlers[msg.MessageType]
		if !found {
			panic(fmt.Sprintf("Actor %s received unsupported message type: %s", a.name, msg.MessageType))
		}
		h(msg.MessageType, msg.Payload)
	}
}

func (a *actor) Send(msgType MessageType, info Payload) Actor {
	a.Cond.L.Lock()
	a.pending = append(a.pending, message{msgType, info})
	a.Cond.Signal()
	a.Cond.L.Unlock()
	return a
}

func (a *actor) Stop() {
	a.Cond.L.Lock()
	a.stopped = true
	a.Cond.L.Unlock()
}