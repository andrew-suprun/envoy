package main

import (
	"flag"
	"fmt"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
	"math/rand"
	"time"
)

var localAddrFlag = flag.String("local", "127.0.0.1:55555", "Local address to bind to.")

func main() {
	flag.Parse()
	msgr := messenger.NewMessenger()
	err := msgr.Subscribe("job", handler)
	if err != nil {
		panic(err)
	}
	_, err = msgr.Join(*localAddrFlag, []string{})
	if err != nil {
		panic(err)
	}
	select {}
}

func handler(topic string, body []byte) []byte {
	time.Sleep(time.Duration(rand.Intn(100)+50) * time.Millisecond)
	result := append([]byte("Received "), body...)
	fmt.Printf(">>> %s\n", string(result))
	return result
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}
