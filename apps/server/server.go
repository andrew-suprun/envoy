package main

import (
	"flag"
	"fmt"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
	"runtime"
	"strings"
)

var localAddrFlag = flag.String("local", "127.0.0.1:55555", "Local address to bind to.")
var remoteAddrFlag = flag.String("remotes", "", "Comma separated remote addresses.")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lmicroseconds)
	flag.Parse()
	msgr := messenger.NewMessenger()
	err := msgr.Subscribe("job", handler)
	if err != nil {
		panic(err)
	}

	remotes := []string{}
	if *remoteAddrFlag != "" {
		remotes = strings.Split(*remoteAddrFlag, ",")
	}

	_, err = msgr.Join(*localAddrFlag, remotes)
	if err != nil {
		log.Printf("Error: %v", err)
	}

	select {}
}

func handler(topic string, body []byte) []byte {
	result := append([]byte("Received "), body...)
	fmt.Println(string(result))
	return result
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}
