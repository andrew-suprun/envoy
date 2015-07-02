package actor

import (
	"fmt"
	"log"
	"sync"
)

type Actor interface {
	RegisterHandler(messageType string, handler Handler) Actor
	Start() Actor
	Send(messageType string, params ...interface{})
	Stop()
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
	running bool
}

type message struct {
	messageType string
	params      []interface{}
}

func (a *actor) RegisterHandler(msgType string, handler Handler) Actor {
	a.handlers[msgType] = handler
	return a
}

func (a *actor) Start() Actor {
	a.Cond.L.Lock()
	if !a.running {
		a.running = true
		go a.run()
	}
	a.Cond.L.Unlock()
	return a
}

func (a *actor) run() {
	for a.running {
		a.Cond.L.Lock()

		if len(a.pending) == 0 {
			a.Cond.Wait()
			a.Cond.L.Unlock()
			continue
		}

		msg := a.pending[0]
		a.pending = a.pending[1:]

		h, found := a.handlers[msg.messageType]

		a.Cond.L.Unlock()

		if found {
			h(msg.messageType, msg.params)
		} else {
			panic(fmt.Sprintf("Actor %s received unsupported message type: %s", a.name, msg.messageType))
		}
	}
}

func (a *actor) Send(msgType string, info ...interface{}) {
	a.Cond.L.Lock()
	a.pending = append(a.pending, message{msgType, info})
	a.Cond.Signal()
	a.Cond.L.Unlock()
}

func (a *actor) Stop() {
	a.Cond.L.Lock()
	a.running = false
	a.Cond.Signal()
	a.Cond.L.Unlock()
}

func (a *actor) logf(format string, params ...interface{}) {
	log.Printf(">>> %s: Actor: "+format, append([]interface{}{a.name}, params...)...)
}
