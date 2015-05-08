package main

import (
	"flag"
	"fmt"
	"github.com/andrew-suprun/envoy/messenger"
)

var localAddrFlag = flag.String("local", "127.0.0.1:55555", "Local address to bind to.")
var remoteAddrFlag = flag.String("remote", ":55556", "Remote address to join cluster.")

func main() {
	flag.Parse()
	msgr, err := messenger.Join(*localAddrFlag, []string{*remoteAddrFlag}, nil)
	if err != nil {
		panic(err)
	}
	msgr.Subscribe("topic", handler)
	if err != nil {
		panic(err)
	}
	select {}
}

func handler(topic string, body []byte) []byte {
	fmt.Printf("~~~ got %s: %s\n", topic, string(body))
	return body
}
