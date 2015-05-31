package main

import (
	"flag"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
	"runtime"
	"strings"
	"sync/atomic"
)

var localAddrFlag = flag.String("local", "", "Local address to bind to.")
var remoteAddrFlag = flag.String("remotes", "", "Comma separated remote addresses.")

const threads = 200

var workers = make(chan struct{}, threads)

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

	for i := 0; i < threads; i++ {
		workers <- struct{}{}
	}

	err = msgr.Join(*localAddrFlag, remotes...)
	if err != nil {
		log.Printf("Error: %v", err)
	}

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
