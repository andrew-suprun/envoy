package main

import (
	"flag"
	"fmt"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var localPortFlag = flag.Int("local", 40000, "local port range start.")
var remoteAddrFlag = flag.String("remote", ":55555", "Remote address to join cluster.")

var written int64
var read int64
var wg sync.WaitGroup
var okCount int64
var errCount int64

func main() {
	start := time.Now()
	flag.Parse()
	wg.Add(200)
	for i := *localPortFlag; i < *localPortFlag+200; i++ {
		thread := i
		go run(thread)
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()
	fmt.Printf("Handled OK %d; Errored %d; Duration %s\n", okCount, errCount, time.Now().Sub(start))
}

func run(thread int) {
	fmt.Printf("start thread #%d\n", thread)
	msgr := messenger.NewMessenger()
	msgr.Subscribe("client", handler)
	local := fmt.Sprintf("localhost:%d", thread)
	joined, err := msgr.Join(local, []string{"localhost:55555"})
	logError(err)

	if len(joined) == 0 {
		fmt.Printf("Failed to join. Exiting\n")
		os.Exit(1)
	}
	for job := 1; job <= 500; job++ {
		out := fmt.Sprintf("--- job #%d from thread #%d ---", job, thread)
		outBuf := []byte(out)
		start := time.Now()
		result, err := msgr.Publish("job", outBuf)
		if err == nil {
			atomic.AddInt64(&okCount, 1)
		} else {
			atomic.AddInt64(&errCount, 1)
		}
		logError(err)
		fmt.Printf("result = '%s'; duration = %s \n", string(result), time.Now().Sub(start))
	}
	wg.Done()
}

func handler(topic string, body []byte) []byte {
	return body
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}
