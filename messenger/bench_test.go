package messenger

import (
	"log"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func Benchmark1(b *testing.B) {
	log.Println("--- Benchmark ---")
	var c int64
	defer func() {
		log.Println("count", c)
	}()

	server := NewMessenger()
	err := server.Subscribe("job", func(topic string, body []byte) []byte {
		// b.Logf("server received topic: '%s' body: '%s'", topic, string(body))
		return body
	})
	if err != nil {
		b.Fatalf("Failed to start server: %v", err)
	}
	server.Join("localhost:55555", time.Second)
	defer server.Leave()

	client := NewMessenger()
	n := client.Join("localhost:44444", time.Second, "localhost:55555")
	if n == 0 {
		b.Fatalf("Server failed to join: %s", err)
	}
	defer client.Leave()

	body := []byte("hello")

	b.SetParallelism(1000)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.AddInt64(&c, 1)
			reply, err := client.Request("job", body, time.Second)
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
