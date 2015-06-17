package main

import (
	"flag"
	"fmt"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const threads = 2000
const msgSize = 1 * 1024
const duration = 60 * time.Second

var localAddrFlag = flag.String("local", "", "Local address.")
var remoteAddrFlag = flag.String("remotes", "", "Comma separated remote addresses.")

var maxDuration time.Duration
var totalDurations time.Duration
var maxDurationMutex sync.Mutex

var (
	timestamp time.Time
	okCount   int64
	errCount  int64
	done      bool
	wg        sync.WaitGroup
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
	msgr, err := messenger.NewMessenger(*localAddrFlag)
	if err != nil {
		fmt.Printf("Failed to join. Exiting\n")
		os.Exit(1)
	}
	msgr.Subscribe("client1", handler)
	remotes := strings.Split(*remoteAddrFlag, ",")
	msgr.Join(remotes...)

	buf := make([]byte, msgSize)
	wg.Add(threads)
	timestamp = time.Now()
	for i := 1; i <= threads; i++ {
		thread := i
		go func(thread int) {
			for job := 1; !done; job++ {
				start := time.Now()

				// err := msgr.Publish("job", buf)
				_, _, err := msgr.Request("job", buf)

				duration := time.Now().Sub(start)
				maxDurationMutex.Lock()
				totalDurations += duration
				if maxDuration < duration {
					maxDuration = duration
				}
				if err == nil {
					c := atomic.AddInt64(&okCount, 1)
					if c%1000 == 0 {
						ts := time.Now()
						log.Println(c, ts.Sub(timestamp))
						timestamp = ts
					}
				} else {
					atomic.AddInt64(&errCount, 1)
				}
				maxDurationMutex.Unlock()

				logError(err)
			}
			wg.Done()
		}(thread)
		time.Sleep(20 * time.Microsecond)
	}
	wg.Wait()
	msgr.Leave()
}

func handler(topic string, body []byte) []byte {
	return body
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v: Stack:\n%s", err, err, string(debug.Stack()))
	}
}
