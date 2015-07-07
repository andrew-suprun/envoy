package proxy

import (
	"errors"
	"fmt"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"log"
	"net"
	"sync/atomic"
	"time"
)

var (
	dialFailed       = errors.New("Dial failed.")
	listenFailed     = errors.New("Listen failed.")
	acceptAfterClose = errors.New("Accept after close.")
)

type Network interface {
	Dial(local, remote HostId, timeout time.Duration) (net.Conn, error)
	Listen(hostId HostId) (net.Listener, error)
}

func NewNetwork() Network {
	return &proxy{}
}

type proxy struct{}

func (n *proxy) Dial(local, remote HostId, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(remote), timeout)
}

func (n *proxy) Listen(hostId HostId) (net.Listener, error) {
	return net.Listen("tcp", string(hostId))
}

type testProxy struct {
	failDial   bool
	failListen bool
	listeners  map[HostId]*listener
	clientInd  int64
}

func NewTestNetwork() Network {
	return &testProxy{
		listeners: make(map[HostId]*listener),
	}
}

func (p *testProxy) Dial(local, remote HostId, timeout time.Duration) (net.Conn, error) {
	ind := atomic.AddInt64(&p.clientInd, 1)
	local = HostId(fmt.Sprintf("%s[%d]", local, ind))
	log.Printf(">>> Dial %s->%s", local, remote)
	if p.failDial {
		return nil, dialFailed
	}
	l := p.listeners[remote]
	if l == nil {
		log.Printf(">>> Dial fail.2 %s; listeners %#v", remote, p.listeners)
		return nil, dialFailed
	}

	c1, c2 := net.Pipe()
	l.acceptChan <- acceptedConn{c2, local}

	return newConn(c1, local, remote), nil
}

func (n *testProxy) Listen(hostId HostId) (net.Listener, error) {
	log.Printf(">>> Listen %s", hostId)
	if n.failListen {
		return nil, listenFailed
	}
	l := newListener(hostId)
	n.listeners[hostId] = l
	return l, nil
}

func newListener(hostId HostId) *listener {
	return &listener{
		hostId:     hostId,
		acceptChan: make(chan acceptedConn),
	}
}

type listener struct {
	hostId     HostId
	acceptChan chan acceptedConn
}

type acceptedConn struct {
	conn   net.Conn
	hostId HostId
}

func (l *listener) Accept() (c net.Conn, err error) {
	conn := <-l.acceptChan
	if conn.conn == nil {
		return nil, acceptAfterClose
	}

	log.Printf(">>> Accepted %s<-%s", l.hostId, conn.hostId)
	return newConn(conn.conn, l.hostId, conn.hostId), nil
}

func (l *listener) Close() error {
	for len(l.acceptChan) > 0 {
		l.acceptChan <- acceptedConn{nil, ""}
	}
	close(l.acceptChan)
	return nil
}

func (l *listener) Addr() net.Addr {
	return addr(l.hostId)
}

type addr HostId

func (a addr) Network() string {
	return "tcp"
}

func (a addr) String() string {
	return string(a)
}

func newConn(_conn net.Conn, local, remote HostId) net.Conn {
	return &conn{_conn, local, remote}
}

type conn struct {
	net.Conn
	local  HostId
	remote HostId
}

func (c *conn) LocalAddr() net.Addr {
	return addr(c.local)
}

func (c *conn) RemoteAddr() net.Addr {
	return addr(c.remote)
}
