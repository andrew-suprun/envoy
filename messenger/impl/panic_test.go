package impl

import (
	. "github.com/andrew-suprun/envoy/messenger"
	"log"
	"testing"
)

func TestPanic(t *testing.T) {
	log.Println("---------------- TestPanic ----------------")

	server := NewMessenger("localhost:50000")
	server.Subscribe("job", panicingHandler)
	err := server.Join()
	if err != nil {
		t.FailNow()
	}
	defer server.Leave()

	client := NewMessenger("localhost:40000")
	err = client.Join("localhost:50000")
	if err != nil {
		t.FailNow()
	}
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
