package main

import (
	"flag"
	"fmt"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
)

var localAddrFlag = flag.String("local", "127.0.0.1:55555", "Local address to bind to.")

func main() {
	flag.Parse()
	msgr := messenger.NewMessenger()
	err := msgr.Subscribe("job", handler)
	if err != nil {
		panic(err)
	}
	joined, err := msgr.Join(*localAddrFlag, []string{})
	if len(joined) > 0 {
		fmt.Printf("~~~ joined %v\n", joined)
	}
	if err != nil {
		panic(err)
	}
	select {}
}

func handler(topic string, body []byte) []byte {
	fmt.Printf("~~~ got %s: %s\n", topic, string(body))
	return append([]byte("Received "), body...)
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}
