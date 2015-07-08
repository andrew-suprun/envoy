package actor

import (
	"fmt"
	"log"
	"sync"
)

func NewActor(handler Handler) Actor {
	actor := &actor{
		handler: handler,
		Cond:    sync.NewCond(&sync.Mutex{}),
	}
	go run(actor)
	return actor
}

type Actor interface {
	Send(message interface{})
}

type MsgStop struct{}

type Handler interface {
	Handle(interface{})
}

type actor struct {
	handler Handler
	pending []interface{}
	*sync.Cond
}

var actors = make(map[string]int)
var actorsMutex sync.Mutex

func run(a *actor) {
	actorsMutex.Lock()
	typ := fmt.Sprintf("%T", a.handler)
	actors[typ] = actors[typ] + 1
	log.Printf("### enter: actors %v", actors)
	actorsMutex.Unlock()

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
		a.handler.Handle(msg)

		if _, ok := msg.(MsgStop); ok {
			actorsMutex.Lock()
			actors[typ] = actors[typ] - 1
			log.Printf("### exit: actors %v", actors)
			actorsMutex.Unlock()
			return
		}
	}
}

func (a *actor) Send(msg interface{}) {
	a.Cond.L.Lock()
	a.pending = append(a.pending, msg)
	a.Cond.Signal()
	a.Cond.L.Unlock()
}
