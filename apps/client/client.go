package main

import (
	"flag"
	"fmt"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

var localAddrFlag = flag.String("local", "", "Local address.")
var remoteAddrFlag = flag.String("remotes", "", "Comma separated remote addresses.")

var okCount int64
var errCount int64

func main() {
	start := time.Now()
	flag.Parse()
	run()
	fmt.Printf("Handled OK %d; Errored %d; Duration %s\n", okCount, errCount, time.Now().Sub(start))
}

func run() {
	msgr := messenger.NewMessenger()
	remotes := strings.Split(*remoteAddrFlag, ",")
	if len(remotes) > 0 {
		joined, _ := msgr.Join(*localAddrFlag, remotes)
		if len(joined) == 0 {
			fmt.Printf("Failed to join. Exiting\n")
			os.Exit(1)
		} else {
			fmt.Printf("Joined %s\n", joined)
		}
	}

	for job := 1; job <= 100000; job++ {
		out := fmt.Sprintf("--- %s: job #%d ---", *localAddrFlag, job)
		outBuf := []byte(out)
		start := time.Now()
		result, err := msgr.Publish("job", outBuf)
		if err == nil {
			atomic.AddInt64(&okCount, 1)
		} else {
			atomic.AddInt64(&errCount, 1)
		}
		logError(err)
		fmt.Printf("%s: %s\n", string(result), time.Now().Sub(start))
	}
}

func handler(topic string, body []byte) []byte {
	return body
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}
