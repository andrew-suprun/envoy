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

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}

func main() {
	flag.Parse()

	msgr, err := messenger.Join("localhost:55556", []string{"localhost:55555"}, nil)
	logError(err)

	result, err := msgr.Publish("aaa", []byte("bbb"))
	logError(err)
	fmt.Printf("result = '%s'\n", string(result))
}
