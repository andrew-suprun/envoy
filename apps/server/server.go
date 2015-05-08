package main

import (
	"flag"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
	"os"
)

var localAddrFlag = flag.String("local", "127.0.0.1:55555", "Local address to bind to.")
var remoteAddrFlag = flag.String("remote", ":55555", "Remote address to join cluster.")

func main() {
	flag.Parse()
	_, err := messenger.Join(*localAddrFlag, []string{*remoteAddrFlag})
	if err != nil {
		log.Printf("### Failed to join Envoy cluster: %v", err)
		os.Exit(1)
	}
	select {}
}
