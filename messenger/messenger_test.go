package messenger

import (
	"log"
	"testing"
	"time"
)

func TestSimpleOneOnOne(t *testing.T) {
	log.Println("---------------- TestSimpleOneOnOne ----------------")
	server := NewMessenger()
	server.Subscribe("job", func(topic string, body []byte) []byte {
		return body
	})
	err := server.Join("localhost:50000", time.Second)
	if err != nil {
		t.Fatalf("Server failed to join: %s", err)
	}
	defer server.Leave()

	client := NewMessenger()

	proxy := newProxy("localhost:30000", "localhost:50000", t)
	defer proxy.close()

	err = client.Join("localhost:40000", time.Second, "localhost:30000")
	if err != nil {
		t.Fatalf("Client failed to join: %s", err)
	}
	defer client.Leave()

	reply, err := client.Request("job", []byte("Hello"), time.Second)
	if err != nil {
		t.Fatalf("Request returned error: %s", err)
	}
	if string(reply) != "Hello" {
		t.Fatalf("Expected: 'Hello'; received '%s'", string(reply))
	}
}
