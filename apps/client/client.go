package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/andrew-suprun/envoy/messenger"
	"github.com/ugorji/go/codec"
	"log"
	"time"
)

var remoteAddrFlag = flag.String("remote", ":55555", "Remote address to join cluster.")

// server.Call(req) resp
// server.Send(msg)
// server.Broadcast(msg)

// server.Join(local, []remote) []result
// server.Leave()
// server.Subscribe([]topic)
// server.Unsubscribe([]topic)
// server.Call(req) resp
// server.Survay(req) []resp
// server.Send(msg)
// server.Broadcast(msg)

var written int64
var read int64

func buildMessage(topic, body string) []byte {
	topicBytes := []byte(topic)
	bodyBytes := []byte(body)
	msg := make([]byte, 0, 8*1024)
	msg = append(msg, byte(len(topicBytes)))
	msg = append(msg, topicBytes...)
	msg = append(msg, bodyBytes...)
	return msg
}

type foo struct {
	Bar    int
	Baz    string
	Foozle []byte
	Ttt    time.Time
	Ddd    time.Duration
}

func logError(err error) {
	if err != nil {
		log.Printf("### Error[%T]: %v", err, err)
	}
}

func main() {
	flag.Parse()

	var ch codec.CborHandle

	f := foo{
		Bar:    42,
		Baz:    "12345",
		Foozle: []byte("abcd"),
		Ttt:    time.Now(),
		Ddd:    time.Second,
	}

	buf := bytes.Buffer{}
	enc := codec.NewEncoder(&buf, &ch)
	err := enc.Encode(f)
	logError(err)

	f2 := foo{}
	dec := codec.NewDecoderBytes(buf.Bytes(), &ch)
	err = dec.Decode(&f2)
	logError(err)

	fmt.Printf("f2 = %+v\n", f2)

	msgr, err := messenger.Join("localhost:55555", []string{"localhost:55556"})
	logError(err)

	msgr.Publish("aaa", []byte("bbb"))

	// raddr, err := net.ResolveUDPAddr("udp", *remoteAddrFlag)
	// if err != nil {
	// 	panic(err)
	// }

	// conn, err := net.DialUDP("udp", nil, raddr)
	// if err != nil {
	// 	log.Printf("### Failed to connect to %s: %v", *remoteAddrFlag, err)
	// 	os.Exit(1)
	// }

	// msg := make([]byte, 8*1024)
	// conn.Write(buildMessage("foo", "bar"))
	// conn.Read(msg)
	// conn.Write(buildMessage("foozle", "xyz"))
	// conn.Read(msg)
}
