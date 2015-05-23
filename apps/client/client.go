package main

import (
	"flag"
	"fmt"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const threads = 10000
const msgSize = 50 * 1024
const duration = 20 * time.Second

var localAddrFlag = flag.String("local", "", "Local address.")
var remoteAddrFlag = flag.String("remotes", "", "Comma separated remote addresses.")

var maxDuration time.Duration
var totalDurations time.Duration
var maxDurationMutex sync.Mutex

var (
	okCount  int64
	errCount int64
	done     bool
	wg       sync.WaitGroup
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lmicroseconds)
	start := time.Now()
	flag.Parse()
	time.AfterFunc(duration, func() {
		done = true
	})
	run()
	totalDuration := time.Now().Sub(start)
	scalability := float64(totalDurations*100) / float64(totalDuration*time.Duration(threads))

	fmt.Printf("Threads: %d\n", threads)
	fmt.Printf("Msg Size: %d\n", msgSize)
	fmt.Printf("MB/s: %f\n", float64(okCount*msgSize)/(1024*1024*totalDuration.Seconds()))
	fmt.Printf("Requests: %d\n", okCount+errCount)
	fmt.Printf("Avg Duration: %f\n", time.Duration(float64(totalDurations)/float64(okCount+errCount)).Seconds())
	fmt.Printf("Max Duration: %f\n", maxDuration.Seconds())
	fmt.Printf("Total Duration: %f\n", totalDuration.Seconds())
	fmt.Printf("Errors: %d\n", errCount)
	fmt.Printf("Scalability: %.2f%%\n", scalability)
}

func run() {
	msgr := messenger.NewMessenger()
	remotes := strings.Split(*remoteAddrFlag, ",")
	msgr.Subscribe("client1", handler)
	if len(remotes) > 0 {
		err := msgr.Join(*localAddrFlag, remotes, time.Second)
		if err != nil {
			fmt.Printf("Failed to join. Exiting\n")
			os.Exit(1)
		}
	}

	msgr.Subscribe("client2", handler)
	msgr.Unsubscribe("client1")

	buf := make([]byte, msgSize)
	wg.Add(threads)
	for i := 1; i <= threads; i++ {
		thread := i
		go func(thread int) {
			for job := 1; !done; job++ {
				start := time.Now()

				err := msgr.Publish("job", buf)
				// _, err := msgr.Request("job", buf, 30*time.Second)

				duration := time.Now().Sub(start)
				maxDurationMutex.Lock()
				totalDurations += duration
				if maxDuration < duration {
					maxDuration = duration
				}
				maxDurationMutex.Unlock()
				if err == nil {
					atomic.AddInt64(&okCount, 1)
				} else {
					atomic.AddInt64(&errCount, 1)
				}
				logError(err)
			}
			wg.Done()
		}(thread)
		time.Sleep(20 * time.Microsecond)
	}
	wg.Wait()
}

func handler(topic string, body []byte) []byte {
	return body
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}
