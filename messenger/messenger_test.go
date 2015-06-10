package messenger

import (
	"errors"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)

var testError = errors.New("test error")

func TestSimpleOneOnOne(t *testing.T) {
	log.Println("---------------- TestSimpleOneOnOne ----------------")

	server, err := NewMessenger("localhost:50000")
	if err != nil {
		t.FailNow()
	}
	defer server.Leave()
	server.Subscribe("job", echo)
	server.Join("localhost:20000")

	client, err := NewMessenger("localhost:40000")
	if err != nil {
		t.FailNow()
	}
	defer client.Leave()
	client.Join("localhost:50000")

	time.Sleep(time.Second)

	for i := 0; i < 20; i++ {
		reply, _, err := client.Request("job", []byte("Hello"), time.Second)
		if err != nil {
			t.Fatalf("Request returned error: %s", err)
		}
		if string(reply) != "Hello" {
			t.Fatalf("Expected: 'Hello'; received '%s'", string(reply))
		}
	}
}

func TestTwoOnTwo(t *testing.T) {
	log.Println("---------------- TestTwoOnTwo ----------------")

	server1, err := NewMessenger("localhost:50000")
	if err != nil {
		t.FailNow()
	}
	defer server1.Leave()
	server1.Subscribe("job", echo1)
	server1.Join()

	server2, err := NewMessenger("localhost:50001")
	if err != nil {
		t.FailNow()
	}
	defer server2.Leave()
	server2.Subscribe("job", echo2)
	server2.Join("localhost:50000")

	client1, err := NewMessenger("localhost:40000")
	if err != nil {
		t.FailNow()
	}
	defer client1.Leave()
	client1.Join("localhost:50000")

	client2, err := NewMessenger("localhost:40001")
	if err != nil {
		t.FailNow()
	}
	defer client2.Leave()
	client2.Join("localhost:50000")

	c := 0
	readMessage = func(conn net.Conn) (*message, error) {
		c++
		if c%10 == 0 {
			log.Printf("--- closing connection %s:%s ---", conn.RemoteAddr(), conn.LocalAddr())
			conn.Close()
		}
		return _readMessage(conn)
	}

	c1s1, c1s2, c2s1, c2s2 := 0, 0, 0, 0
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for i := 0; i < 100; i++ {
			reply, _, err := client1.Request("job", []byte("Hello1"), time.Second)
			rep := string(reply)
			log.Println(rep)
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
			reply, _, err := client2.Request("job", []byte("Hello2"), time.Second)
			rep := string(reply)
			log.Println(rep)
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

func echo(topic string, body []byte) []byte {
	return body
}

func echo1(topic string, body []byte) []byte {
	return []byte("server:1 " + string(body))
}

func echo2(topic string, body []byte) []byte {
	return []byte("server:2 " + string(body))
}
