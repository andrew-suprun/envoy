package impl

import (
	"errors"
	. "github.com/andrew-suprun/envoy/messenger"
	"log"
	"sync"
	"testing"
	"time"
)

var testError = errors.New("test error")

func TestOneOnOne(t *testing.T) {
	log.Println("---------------- TestOneOnOne ----------------")

	server := NewMessenger("localhost:50000")
	defer server.Leave()
	err := server.Join("localhost:20000")
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}
	defer server.Leave()
	server.Subscribe("job", echo)

	client := NewMessenger("localhost:40000")
	err = client.Join("localhost:50000")
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}
	defer client.Leave()

	for i := 0; i < 20; i++ {
		reply, _, err := client.Request("job", []byte("Hello"))
		if err != nil {
			log.Fatalf("Request returned error: %s", err)
		}
		if string(reply) != "Hello" {
			log.Fatalf("Expected: 'Hello'; received '%s'", string(reply))
		}
	}
}

func TestOneOnThree(t *testing.T) {
	log.Println("---------------- TestOneOnThree ----------------")

	Timeout = time.Duration(5 * time.Second)

	server1 := NewMessenger("localhost:50001")
	defer server1.Leave()
	err := server1.Join()
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}
	server1.Subscribe("job", echo1)

	server2 := NewMessenger("localhost:50002")
	defer server2.Leave()
	server2.Subscribe("job", echo2)
	err = server2.Join("localhost:50001")
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}

	server3 := NewMessenger("localhost:50003")
	defer server3.Leave()
	err = server3.Join("localhost:50002")
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}
	server3.Subscribe("job", echo3)

	client := NewMessenger("localhost:40000")
	defer client.Leave()
	err = client.Join("localhost:50002")
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}

	s1, s2, s3 := 0, 0, 0
	for i := 0; i < 100; i++ {
		reply, _, err := client.Request("job", []byte("Hello"))
		rep := string(reply)
		log.Printf("client: received reply '%s'; err = %v", rep, err)
		if err != nil {
			t.Errorf("Request returned error: %s", err)
			break
		}
		switch rep {
		case "server:1 Hello":
			s1++
		case "server:2 Hello":
			s2++
		case "server:3 Hello":
			s3++
		default:
			t.Errorf("Expected: either 'server:X Hello'; received '%s'", string(reply))
			break
		}
	}
	log.Printf("counts: s1: %d, s2: %d, s3: %d", s1, s2, s3)
	if s1 < 10 || s2 < 10 || s3 < 10 || s1+s2+s3 != 100 {
		t.Errorf("Wrong counts: %d, %d, %d", s1, s2, s3)
	}
}

func TestTwoOnTwo(t *testing.T) {
	log.Println("---------------- TestTwoOnTwo ----------------")

	server1 := NewMessenger("localhost:50000")
	defer server1.Leave()
	server1.Subscribe("job", echo1)
	err := server1.Join()
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}

	server2 := NewMessenger("localhost:50001")
	defer server2.Leave()
	server2.Subscribe("job", echo2)
	err = server2.Join("localhost:50000")
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}

	client1 := NewMessenger("localhost:40000")
	defer client1.Leave()
	err = client1.Join("localhost:50000")
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}

	client2 := NewMessenger("localhost:40001")
	defer client2.Leave()
	err = client2.Join("localhost:50000")
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}

	c1s1, c1s2, c2s1, c2s2 := 0, 0, 0, 0
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for i := 0; i < 100; i++ {
			reply, _, err := client1.Request("job", []byte("Hello1"))
			rep := string(reply)
			log.Printf("client.1: received reply '%s'; err = %v", rep, err)
			if err != nil {
				t.Errorf("Request returned error: %s", err)
				break
			}
			switch rep {
			case "server:1 Hello1":
				c1s1++
			case "server:2 Hello1":
				c1s2++
			default:
				t.Errorf("Expected: 'Hello1'; received '%s'", string(reply))
				break
			}
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < 100; i++ {
			reply, _, err := client2.Request("job", []byte("Hello2"))
			rep := string(reply)
			log.Printf("client.2: received reply '%s'; err = %v", rep, err)
			if err != nil {
				t.Errorf("Request returned error: %s", err)
				break
			}
			switch rep {
			case "server:1 Hello2":
				c2s1++
			case "server:2 Hello2":
				c2s2++
			default:
				t.Errorf("Expected: 'Hello1'; received '%s'", string(reply))
				break
			}
		}
		wg.Done()
	}()
	wg.Wait()
	log.Printf("counts: c1s1: %d, c1s2: %d, c2s1: %d, c2s2: %d", c1s1, c1s2, c2s1, c2s2)
	if c1s1 < 25 || c1s2 < 25 || c2s1 < 25 || c2s2 < 25 || c1s1+c1s2 != 100 || c2s1+c2s2 != 100 {
		t.Errorf("Wrong counts")
	}
}

func TestDisconnect(t *testing.T) {
	log.Println("---------------- TestDisconnect ----------------")

	Timeout = 2 * time.Second

	server1 := NewMessenger("localhost:50000")
	defer server1.Leave()
	server1.Subscribe("job", echo1)
	err := server1.Join()
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}

	server2 := NewMessenger("localhost:50001")
	defer server2.Leave()
	server2.Subscribe("job", echo2)
	err = server2.Join("localhost:50000")
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}

	client1 := NewMessenger("localhost:40000")
	defer client1.Leave()
	err = client1.Join("localhost:50000")
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}

	client2 := NewMessenger("localhost:40001")
	defer client2.Leave()
	err = client2.Join("localhost:50000")
	if err != nil {
		log.Fatalf("Failed to join: %v", err)
	}

	// TODO
	// var c int64 = 0
	// var cc int64
	// readMessage = func(conn net.Conn) (*message, error) {
	// 	cc = atomic.AddInt64(&c, 1)
	// 	if cc%500 == 0 {
	// 		log.Printf("### closing connection %s:%s [%d] ---", conn.RemoteAddr(), conn.LocalAddr(), cc)
	// 		conn.Close()
	// 	}
	// 	return _readMessage(conn)
	// }

	c1s1, c1s2, c2s1, c2s2 := 0, 0, 0, 0
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for i := 0; i < 1000; i++ {
			// cc = atomic.AddInt64(&c, 1)
			// log.Printf("client.1: sending 'Hello1'; cc = %d", cc)
			reply, _, err := client1.Request("job", []byte("Hello1"))
			rep := string(reply)
			log.Printf("client.1: received reply '%s'; err = %v", rep, err)
			if err != nil {
				t.Errorf("Request returned error: %s", err)
				break
			}
			switch rep {
			case "server:1 Hello1":
				c1s1++
			case "server:2 Hello1":
				c1s2++
			default:
				t.Errorf("Expected: 'Hello1'; received '%s'", string(reply))
				break
			}
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			// cc = atomic.AddInt64(&c, 1)
			// log.Printf("client.2: sending 'Hello2'; cc = %d", cc)
			reply, _, err := client2.Request("job", []byte("Hello2"))
			rep := string(reply)
			log.Printf("client.2: received reply '%s'; err = %v", rep, err)
			if err != nil {
				t.Errorf("Request returned error: %s", err)
				break
			}
			switch rep {
			case "server:1 Hello2":
				c2s1++
			case "server:2 Hello2":
				c2s2++
			default:
				t.Errorf("Expected: 'Hello1'; received '%s'", string(reply))
				break
			}
		}
		wg.Done()
	}()
	wg.Wait()
	log.Printf("counts: c1s1: %d, c1s2: %d, c2s1: %d, c2s2: %d", c1s1, c1s2, c2s1, c2s2)
	if c1s1 < 25 || c1s2 < 25 || c2s1 < 25 || c2s2 < 25 || c1s1+c1s2 != 1000 || c2s1+c2s2 != 1000 {
		t.Errorf("Wrong counts")
	}
}

func TestPublish(t *testing.T) {
	log.Println("---------------- TestPublish ----------------")

	wg := sync.WaitGroup{}
	wg.Add(20)

	server := NewMessenger("localhost:50000")
	err := server.Join()
	if err != nil {
		t.FailNow()
	}
	defer server.Leave()
	server.Subscribe("job", func(topic string, body []byte, _ MessageId) []byte {
		log.Printf("published = %s/%s", topic, string(body))
		server.Publish("result", body)
		return body
	})

	client := NewMessenger("localhost:40000")
	client.Subscribe("result", func(topic string, body []byte, _ MessageId) []byte {
		log.Printf("client result = %s/%s", topic, string(body))
		wg.Done()
		return body
	})
	err = client.Join("localhost:50000")
	if err != nil {
		t.FailNow()
	}
	defer client.Leave()

	for i := 0; i < 20; i++ {
		_, err := client.Publish("job", []byte("Hello"))
		if err != nil {
			log.Fatalf("Request returned error: %s", err)
		}
	}
	wg.Wait()
}

func echo(topic string, body []byte, _ MessageId) []byte {
	return body
}

func echo1(topic string, body []byte, _ MessageId) []byte {
	return []byte("server:1 " + string(body))
}

func echo2(topic string, body []byte, _ MessageId) []byte {
	return []byte("server:2 " + string(body))
}

func echo3(topic string, body []byte, _ MessageId) []byte {
	return []byte("server:3 " + string(body))
}
