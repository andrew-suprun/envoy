package messenger

import (
	. "github.com/andrew-suprun/envoy"
	"log"
	"testing"
)

func TestPanic(t *testing.T) {
	log.Println("---------------- TestPanic ----------------")

	server, err := NewMessenger("localhost:50000")
	if err != nil {
		t.FailNow()
	}
	defer server.Leave()
	server.Subscribe("job", panicingHandler)
	server.Join()

	client, err := NewMessenger("localhost:40000")
	if err != nil {
		t.FailNow()
	}
	defer client.Leave()
	client.Join("localhost:50000")

	reply, _, err := client.Request("job", []byte("Hello"))
	log.Printf("TestPanic: reply = %s; err = %v", string(reply), err)
	if err != PanicError {
		t.Fatalf("Server didn't panic")
	}
}

func panicingHandler(topic string, body []byte, _ MessageId) []byte {
	panic("foo")
}
