package reader

import (
	"github.com/andrew-suprun/envoy/actor"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"github.com/andrew-suprun/envoy/messenger/writer"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)

func TestOne(t *testing.T) {
	log.SetFlags(log.Lmicroseconds)
	wg := &sync.WaitGroup{}
	wg.Add(4)

	conn1, conn2 := net.Pipe()
	rActor := actor.NewActor(readHandler{conn1, t, wg, false})
	wActor := actor.NewActor(writeHandler{conn2, t, wg, false})

	reader := NewReader("reader", conn1, rActor)

	writer := writer.NewWriter("writer", conn2, wActor)

	writer.Send(&Message{
		Topic:       "_topic",
		Body:        []byte("hello"),
		MessageId:   NewId(),
		MessageType: Request,
	})
	time.Sleep(time.Millisecond)
	conn1.Close()
	writer.Send(&Message{
		Topic:       "_topic2",
		Body:        []byte("hello2"),
		MessageId:   NewId(),
		MessageType: Publish,
	})
	wg.Wait()
	reader.Send(actor.MsgStop{})
	writer.Send(actor.MsgStop{})
}

type readHandler struct {
	conn      net.Conn
	t         *testing.T
	wg        *sync.WaitGroup
	readHello bool
}

func (h readHandler) Handle(msg interface{}) {
	switch m := msg.(type) {
	case MsgMessageRead:
		if !h.readHello && m.Msg.Topic == "_topic" {
			log.Printf("read-handler: read msg: %+v", m)
			h.readHello = true
		} else {
			log.Printf("read-handler: expected _topic; got [%T]: %#v", m, m)
			h.t.FailNow()
		}
	case MsgNetworkError:
		log.Printf("read-handler: got an error: %v", m)
	default:
		log.Printf("read-handler: neither MsgMessageRead nor MsgNetworkError[%T]: %#v", m, m)
		h.t.FailNow()
	}
	h.wg.Done()
}

type writeHandler struct {
	conn       net.Conn
	t          *testing.T
	wg         *sync.WaitGroup
	wroteHello bool
}

func (h writeHandler) Handle(msg interface{}) {
	switch m := msg.(type) {
	case writer.MsgMessageWritten:
		if !h.wroteHello {
			log.Printf("write-handler: wrote msg: %+v", m)
			h.wroteHello = true
		} else {
			log.Printf("write-handler: expected error; got [%T]: %#v", m, m)
			h.t.FailNow()
		}
	case MsgNetworkError:
		log.Printf("write-handler: got an error: %v", m)
	default:
		log.Printf("write-handler: neither MsgMessageWritten nor MsgNetworkError[%T]: %#v", m, m)
		h.t.FailNow()
	}
	h.wg.Done()
}
