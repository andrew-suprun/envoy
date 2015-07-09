package messenger

import (
	. "github.com/andrew-suprun/envoy/messenger/common"
	"log"
	"runtime"
	"sync/atomic"
	"testing"
)

func Benchmark1(b *testing.B) {
	log.Println("--- Benchmark ---")
	var c int64
	defer func() {
		log.Println("count", c)
	}()

	server, err := NewMessenger("localhost:55555")
	if err != nil {
		b.Fatalf("Failed to join: %v", err)
	}
	server.Subscribe("job", func(topic string, body []byte, _ MessageId) []byte {
		// b.Logf("server received topic: '%s' body: '%s'", topic, string(body))
		return body
	})
	server.Join()
	defer server.Leave()

	client, err := NewMessenger("localhost:44444")
	if err != nil {
		b.Fatalf("Failed to join: %v", err)
	}

	client.Join("localhost:55555")
	defer client.Leave()

	body := []byte("hello")

	b.SetParallelism(1000)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.AddInt64(&c, 1)
			reply, _, err := client.Request("job", body)
			if err != nil {
				b.Fatalf("Request returned an error: %v", err)
			}
			if string(reply) != "hello" {
				b.Fatalf("Wrong reply: '%s'", string(reply))
			}
		}
	})
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lmicroseconds)
}
