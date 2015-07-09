package messenger

import (
	"fmt"
	"github.com/andrew-suprun/envoy/actor"
	"github.com/andrew-suprun/envoy/future"
	"github.com/andrew-suprun/envoy/messenger/client"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"github.com/andrew-suprun/envoy/messenger/proxy"
	"github.com/andrew-suprun/envoy/messenger/server"
	"net"
)

//////////////// Impl //////////////////

func NewMessenger(local string) (Messenger, error) {
	localAddr, err := resolveAddr(local)
	if err != nil {
		return nil, err
	}
	msgr := &messenger{
		hostId: localAddr,
	}

	msgr.server, err = server.NewServer(localAddr, _proxy)
	if err != nil {
		return nil, err
	}
	return msgr, nil
}

func (msgr *messenger) Join(remotes ...string) {
	resolved := make([]HostId, 0, len(remotes))
	for _, remote := range remotes {
		remoteAddr, err := resolveAddr(remote)
		if err == nil {
			resolved = append(resolved, remoteAddr)
		} else {
			Log.Errorf("Cannot resolve address %s. Ignoring.", remote)
		}
	}

	msgr.client = client.NewClent(msgr.hostId, _proxy)
	msgr.server.Send(server.MsgClient{msgr.client})

	result := future.NewFuture()
	msgr.client.Send(client.MsgJoin{resolved, result})

	result.Value()
}

func (msgr *messenger) Leave() {
	f := futures(2)
	msgr.server.Send(MsgLeave{f[0]})
	msgr.client.Send(MsgLeave{f[1]})

	waitFutures(f)
}

func (msgr *messenger) Publish(topic string, body []byte) (MessageId, error) {
	result := future.NewFuture()
	msgr.client.Send(&client.MsgPublish{
		Msg: Message{
			Topic:       Topic(topic),
			Body:        body,
			MessageType: Publish,
			MessageId:   NewId(),
		},
		Result: result,
	})

	switch msg := result.Value().(type) {
	case *Message:
		return msg.MessageId, nil
	case error:
		return &MsgId{}, msg
	default:
		panic(fmt.Sprintf("messenger.Publish returned [%T] %#v", msg, msg))
	}
}

func (msgr *messenger) Request(topic string, body []byte) ([]byte, MessageId, error) {
	result := future.NewFuture()
	msgr.client.Send(&client.MsgPublish{
		Msg: Message{
			Topic:       Topic(topic),
			Body:        body,
			MessageType: Request,
			MessageId:   NewId(),
		},
		Result: result,
	})

	switch msg := result.Value().(type) {
	case *Message:
		return msg.Body, msg.MessageId, nil
	case error:
		return nil, &MsgId{}, msg
	default:
		panic(fmt.Sprintf("messenger.Request returned [%T] %#v", msg, msg))
	}
}

func (msgr *messenger) Broadcast(topic string, body []byte) (MessageId, error) {
	result := future.NewFuture()
	msgr.client.Send(&client.MsgPublish{
		Msg: Message{
			Topic:       Topic(topic),
			Body:        body,
			MessageType: Broadcast,
			MessageId:   NewId(),
		},
		Result: result,
	})

	switch msg := result.Value().(type) {
	case *Message:
		return msg.MessageId, nil
	case error:
		return &MsgId{}, msg
	default:
		panic(fmt.Sprintf("messenger.Broadcast returned [%T] %#v", msg, msg))
	}
}

func (msgr *messenger) Survey(topic string, body []byte) ([][]byte, MessageId, error) {
	result := future.NewFuture()
	msgr.client.Send(&client.MsgPublish{
		Msg: Message{
			Topic:       Topic(topic),
			Body:        body,
			MessageType: Survey,
			MessageId:   NewId(),
		},
		Result: result,
	})

	switch msg := result.Value().(type) {
	case *Message:
		return [][]byte{msg.Body}, msg.MessageId, nil // TODO: fix
	case error:
		return nil, &MsgId{}, msg
	default:
		panic(fmt.Sprintf("messenger.Survey returned [%T] %#v", msg, msg))
	}
}

func (msgr *messenger) Subscribe(topic string, handler Handler) {
	msgr.server.Send(server.MsgSubscribe{Topic(topic), handler})
}

func (msgr *messenger) Unsubscribe(topic string) {
	msgr.server.Send(server.MsgUnsubscribe{Topic(topic)})
}

func resolveAddr(addr string) (HostId, error) {
	resolved, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return "", err
	}
	if resolved.IP == nil {
		return HostId(fmt.Sprintf("127.0.0.1:%d", resolved.Port)), nil
	}
	return HostId(resolved.String()), nil
}

func futures(n int) []future.Future {
	result := make([]future.Future, n)
	for i := 0; i < n; i++ {
		result[i] = future.NewFuture()
	}
	return result
}

func waitFutures(futures []future.Future) {
	for _, f := range futures {
		f.Value()
	}
}

type messenger struct {
	hostId HostId
	server actor.Actor
	client actor.Actor
}

var _proxy = proxy.NewNetwork()

func test(params proxy.ProxyParams) {
	_proxy = proxy.NewTestNetwork(params)
}
