package main

import (
	"flag"
	"github.com/andrew-suprun/envoy/messenger"
	"log"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

var localAddrFlag = flag.String("local", "localhost:55555", "Local address to bind to.")
var remoteAddrFlag = flag.String("remotes", "localhost:44444", "Comma separated remote addresses.")

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

	err = msgr.Join(*localAddrFlag, time.Second, remotes...)
	if err != nil {
		log.Printf("Error: %v", err)
	}

	select {}
}

var count int64

func handler(topic string, body []byte) []byte {
	c := atomic.AddInt64(&count, 1)
	// result := append([]byte(fmt.Sprintf("[%d]: ", c)), body...)
	if c%1000 == 0 {
		log.Println(c)
	}
	// time.Sleep(time.Duration(rand.Intn(3000)+2000) * time.Millisecond)
	return body
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}
