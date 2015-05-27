package messenger

import (
	"log"
	"sync"
	"testing"
	"time"
)

func TestSimpleOneOnOne(t *testing.T) {
	log.Println("---------------- TestSimpleOneOnOne ----------------")

	server := NewMessenger()
	defer server.Leave()
	server.Subscribe("job", echo)
	server.Join("localhost:50000", time.Second)

	client := NewMessenger()
	defer client.Leave()
	client.Join("localhost:40000", time.Second, "localhost:50000")

	for i := 0; i < 20; i++ {
		reply, err := client.Request("job", []byte("Hello"), time.Second)
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

	server1 := NewMessenger()
	defer server1.Leave()
	server1.Subscribe("job", echo1)
	server1.Join("localhost:50000", time.Second)

	server2 := NewMessenger()
	defer server2.Leave()
	server2.Subscribe("job", echo2)
	server2.Join("localhost:50001", time.Second, "localhost:50000")

	client1 := NewMessenger()
	defer client1.Leave()
	client1.Join("localhost:40000", time.Second, "localhost:50000")

	client2 := NewMessenger()
	defer client2.Leave()
	client2.Join("localhost:40001", time.Second, "localhost:50000")

	c1s1, c1s2, c2s1, c2s2 := 0, 0, 0, 0
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for i := 0; i < 20; i++ {
			reply, err := client1.Request("job", []byte("Hello1"), time.Second)
			rep := string(reply)
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
		for i := 0; i < 20; i++ {
			reply, err := client2.Request("job", []byte("Hello2"), time.Second)
			rep := string(reply)
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
	if c1s1 == 0 || c1s2 == 0 || c2s1 == 0 || c2s2 == 0 || c1s1+c1s2 != 20 || c2s1+c2s2 != 20 {
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
