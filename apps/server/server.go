package main

import (
	"flag"
	"fmt"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
	"math/rand"
	"strings"
	"time"
)

var localAddrFlag = flag.String("local", "127.0.0.1:55555", "Local address to bind to.")
var remoteAddrFlag = flag.String("remote", "", "Remote address to join cluster.")

func main() {
	flag.Parse()
	msgr := messenger.NewMessenger()
	err := msgr.Subscribe("job", handler)
	if err != nil {
		panic(err)
	}

	remotes := []string{}
	if *remoteAddrFlag != "" {
		remotes = strings.Split(*remoteAddrFlag, ",")
		log.Printf("Joining %s", remotes)
	}

	joined, err := msgr.Join(*localAddrFlag, remotes)
	if len(joined) > 0 {
		log.Printf("Joined %s", joined)
	}
	if err != nil {
		log.Printf("Error: %v", err)
	}

	select {}
}

func handler(topic string, body []byte) []byte {
	time.Sleep(time.Duration(rand.Intn(30000)+20000) * time.Microsecond)
	result := append([]byte("Received "), body...)
	fmt.Printf(">>> %s\n", string(result))
	return result
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}
