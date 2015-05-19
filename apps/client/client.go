package main

import (
	"flag"
	"fmt"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

const (
	threads = 10000
	jobs    = 10
)

var localAddrFlag = flag.String("local", "", "Local address.")
var remoteAddrFlag = flag.String("remotes", "", "Comma separated remote addresses.")

var okCount int64
var errCount int64

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lmicroseconds)
	start := time.Now()
	flag.Parse()
	run()
	time.Sleep(time.Minute)
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
		}
	}

	for i := 1; i <= threads; i++ {
		thread := i
		go func(thread int) {
			for job := 1; ; job++ {
				out := fmt.Sprintf("--- %s: thread #%d job #%d ---", *localAddrFlag, thread, job)
				outBuf := []byte(out)
				start := time.Now()
				result, err := msgr.Publish("job", outBuf)
				if err == nil {
					atomic.AddInt64(&okCount, 1)
				} else {
					atomic.AddInt64(&errCount, 1)
				}
				logError(err)
				fmt.Printf("%s: %f\n", string(result), time.Now().Sub(start).Seconds())
				time.Sleep(time.Duration(rand.Intn(1000)+1000) * time.Millisecond)
			}
		}(thread)
		time.Sleep(200 * time.Microsecond)
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
