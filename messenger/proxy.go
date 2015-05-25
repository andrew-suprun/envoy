package messenger

import (
	"bytes"
	"log"
	"net"
	"testing"
)

type proxy struct {
	*testing.T
	net.Listener
	serverConn net.Conn
	proxyConns map[string]*proxyConn
	error
	closing bool
}

type proxyConn struct {
	*proxy
	direction string
	source    net.Conn
	target    net.Conn
}

func newProxy(local, remote string, t *testing.T) *proxy {
	p := &proxy{T: t, proxyConns: make(map[string]*proxyConn)}
	p.serverConn, p.error = net.Dial("tcp", remote)
	if p.error != nil {
		p.Fatalf("Proxy: Failed to connect to %s", remote)
	}

	p.Listener, p.error = net.Listen("tcp", local)
	if p.error != nil {
		p.Fatalf("Proxy: Failed to listen on %s", local)
	}

	go p.listen()

	return p
}

func (p *proxy) listen() {
	log.Printf("Proxy: Listening on %s; local = %s", p.Addr(), p.serverConn.LocalAddr())
	for {
		clientConn, err := p.Accept()
		if err != nil {
			p.error = err
			if p.closing {
				break
			}
			p.Fatalf("Proxy: Failed to accept connection: %s", p.error)
			continue
		}
		client := clientConn.LocalAddr().String()
		server := p.serverConn.RemoteAddr().String()
		c2sDir := client + "->" + server
		s2cDir := server + "->" + client

		c2s := &proxyConn{proxy: p, direction: c2sDir, source: clientConn, target: p.serverConn}
		s2c := &proxyConn{proxy: p, direction: s2cDir, source: p.serverConn, target: clientConn}

		p.proxyConns[c2sDir] = c2s
		p.proxyConns[s2cDir] = s2c

		go c2s.readLoop()
		go s2c.readLoop()
	}
}

func (pc *proxyConn) readLoop() {
	log.Printf("Proxy: entered read loop: %s", pc.direction)
	for {
		msg, err := pc.readMessage(pc.source)
		if err != nil {
			pc.proxy.error = err
			log.Printf("Proxy: Failed to read from source %s: %v", pc.source, err)
			continue
		}

		log.Printf("Proxy: %s: %s", pc.direction, msg.MessageType)

		switch msg.MessageType {
		case join:
		}

		err = pc.writeMessage(pc.target, msg)
		if err != nil {
			pc.proxy.error = err
			log.Printf("Proxy: Failed write to target %s: %v", pc.target, err)
		}
	}
}

func (pc *proxyConn) readMessage(from net.Conn) (*message, error) {
	lenBuf := make([]byte, 4)
	readBuf := lenBuf

	for len(readBuf) > 0 {
		n, err := from.Read(readBuf)
		if err != nil {
			return nil, err
		}
		readBuf = readBuf[n:]
	}

	msgSize := getUint32(lenBuf)
	msgBytes := make([]byte, msgSize)
	readBuf = msgBytes
	for len(readBuf) > 0 {
		n, err := from.Read(readBuf)
		if err != nil {
			return nil, err
		}
		readBuf = readBuf[n:]
	}
	msgBuf := bytes.NewBuffer(msgBytes)
	msg := &message{}
	decode(msgBuf, msg)
	log.Printf("Proxy: pc.readMessage[%s]: '%s' from %s", pc.direction, msg.MessageType, from.RemoteAddr())
	return msg, nil
}

func (pc *proxyConn) writeMessage(to net.Conn, msg *message) error {
	log.Printf("Proxy: pc.writeMessage[%s]: '%s' to %s", pc.direction, msg.MessageType, to.RemoteAddr())
	buf := bytes.NewBuffer(make([]byte, 4, 128))
	encode(msg, buf)
	bufSize := buf.Len()
	putUint32(buf.Bytes(), uint32(bufSize-4))
	n, err := to.Write(buf.Bytes())
	if err == nil && n != bufSize {
		log.Printf("Proxy: Failed to write to %s: %v. ", to.RemoteAddr(), err)
	}
	return err
}

func (p *proxy) close() {
	p.closing = true
	p.Close()
}
