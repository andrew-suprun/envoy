package main

import (
	"flag"
	"fmt"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
)

var remoteAddrFlag = flag.String("remote", ":55555", "Remote address to join cluster.")

var written int64
var read int64

func main() {

	flag.Parse()

	msgr := messenger.NewMessenger()
	msgr.Subscribe("client", handler)
	err := msgr.Join("localhost:55556", []string{"localhost:55555"})
	logError(err)

	result, err := msgr.Publish("job", []byte("bbb"))
	logError(err)
	fmt.Printf("result = '%s'\n", string(result))
}

func handler(topic string, body []byte) []byte {
	fmt.Printf("~~~ got %s: %s\n", topic, string(body))
	return body
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}
