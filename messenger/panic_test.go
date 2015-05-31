package messenger

import (
	"log"
	"testing"
	"time"
)

func TestPanic(t *testing.T) {
	log.Println("---------------- TestPanic ----------------")

	server := NewMessenger()
	defer server.Leave()
	server.Subscribe("job", panicingHandler)
	server.Join("localhost:50000", "localhost:20000")

	client := NewMessenger()
	defer client.Leave()
	client.Join("localhost:40000", "localhost:50000")

	reply, err := client.Request("job", []byte("Hello"), time.Second)
	log.Printf("TestPanic: reply = %s; err = %v", string(reply), err)
	if err != PanicError {
		t.Fatalf("Server didn't panic", err)
	}
}

func panicingHandler(topic string, body []byte) []byte {
	panic("foo")
}
