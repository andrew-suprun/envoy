package main

import (
	"flag"
	"fmt"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

var localAddrFlag = flag.String("local", "", "Local address to bind to.")
var remoteAddrFlag = flag.String("remotes", "", "Comma separated remote addresses.")

const threads = 200

var workers = make(chan struct{}, threads)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lmicroseconds)
	flag.Parse()
	msgr, err := messenger.NewMessenger(*localAddrFlag)
	if err != nil {
		log.Printf("Error: %v", err)
	}
	msgr.Subscribe("job", handler)

	remotes := []string{}
	if *remoteAddrFlag != "" {
		remotes = strings.Split(*remoteAddrFlag, ",")
	}

	for i := 0; i < threads; i++ {
		workers <- struct{}{}
	}

	msgr.Join(remotes...)

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for {
			<-sigs
			fmt.Println("\nLeaving...")
			msgr.Leave()
			fmt.Println("Bye.")
			os.Exit(0)
		}
	}()

	select {}
}

var count int64

func handler(topic string, body []byte) []byte {
	<-workers
	c := atomic.AddInt64(&count, 1)
	if c%1000 == 0 {
		log.Println(c)
	}
	time.Sleep(time.Duration(rand.Intn(100)+100) * time.Millisecond)
	workers <- struct{}{}
	return body
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}
