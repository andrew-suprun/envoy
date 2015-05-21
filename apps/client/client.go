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

const threads = 100000
const duration = 5 * time.Second

var localAddrFlag = flag.String("local", "", "Local address.")
var remoteAddrFlag = flag.String("remotes", "", "Comma separated remote addresses.")

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
	fmt.Printf("Handled OK %d; Errored %d; Duration %s\n", okCount, errCount, time.Now().Sub(start))
}

func run() {
	msgr := messenger.NewMessenger()
	remotes := strings.Split(*remoteAddrFlag, ",")
	if len(remotes) > 0 {
		err := msgr.Join(*localAddrFlag, remotes, time.Second)
		if err != nil {
			fmt.Printf("Failed to join. Exiting\n")
			os.Exit(1)
		}
	}

	buf := make([]byte, 100*1024)
	wg.Add(threads)
	for i := 1; i <= threads; i++ {
		thread := i
		go func(thread int) {
			for job := 1; !done; job++ {
				// err := msgr.Publish("job", buf)
				_, err := msgr.Request("job", buf, 10*time.Second)
				if err == nil {
					atomic.AddInt64(&okCount, 1)
				} else {
					atomic.AddInt64(&errCount, 1)
				}
				logError(err)
				// fmt.Printf("%s: %f\n", string(result), time.Now().Sub(start).Seconds())
				// time.Sleep(time.Duration(rand.Intn(100)+100) * time.Microsecond)
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
