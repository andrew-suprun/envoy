package messenger

import (
	. "github.com/andrew-suprun/envoy/messenger/common"
	"log"
	"testing"
)

func TestPanic(t *testing.T) {
	log.Println("---------------- TestPanic ----------------")

	server, err := NewMessenger("localhost:50000")
	if err != nil {
		t.FailNow()
	}
	server.Subscribe("job", panicingHandler)
	server.Join()
	defer server.Leave()

	client, err := NewMessenger("localhost:40000")
	if err != nil {
		t.FailNow()
	}
	client.Join("localhost:50000")
	defer client.Leave()

	reply, _, err := client.Request("job", []byte("Hello"))
	log.Printf("TestPanic: reply = %s; err = %v", string(reply), err)
	if err != PanicError {
		log.Fatalf("Server didn't panic")
	}
}

func panicingHandler(topic string, body []byte, _ MessageId) []byte {
	panic("foo")
}
