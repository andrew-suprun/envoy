package messenger

import (
	. "github.com/andrew-suprun/envoy"
	"github.com/andrew-suprun/envoy/future"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"github.com/andrew-suprun/envoy/messenger/peers"
)

func init() {
	NewMessenger = newMessenger
}

type messenger struct {
	peers Sender
}

func newMessenger(local string) (Messenger, error) {
	peers, err := peers.NewPeers(local)
	if err != nil {
		return nil, err
	}
	return &messenger{peers: peers}, nil
}

func (msgr *messenger) Join(remotes ...string) error {
	result := future.NewFuture()
	msgr.peers.Send("join", remotes, result)
	return result.Error()
}

func (msgr *messenger) Leave() {
	result := future.NewFuture()
	msgr.peers.Send("leave", result)
	result.Error()
}

func (msgr *messenger) Publish(topic string, body []byte) (MessageId, error) {
	// result := future.NewFuture()
	// msgr.peers.Send("publish", topic, body, result)
	// return result.Value().(MessageId), result.Error()

	result := future.NewFuture()
	msgr.peers.Send("publish", topic, body, result)
	err := result.Error()
	if err != nil {
		return &MsgId{}, err
	}
	return result.Value().(MessageId), result.Error()
}

func (msgr *messenger) Request(topic string, body []byte) ([]byte, MessageId, error) {
	result := future.NewFuture()
	msgr.peers.Send("request", topic, body, result)
	err := result.Error()
	if err != nil {
		return nil, &MsgId{}, err
	}
	resMsg := result.Value().(*Message)
	return resMsg.Body, resMsg.MessageId, result.Error()
}

func (msgr *messenger) Broadcast(topic string, body []byte) (MessageId, error) {
	replies := future.NewFuture()
	msgr.peers.Send("broadcast", topic, body, replies)
	err := replies.Error()
	if err != nil {
		return MsgId{}, err
	}

	var msgId MessageId
	for _, reply := range replies.Value().([]future.Future) {
		if err == nil {
			err = reply.Error()
			msgId, _ = reply.Value().(MessageId)
		}
	}
	return msgId, err
}

func (msgr *messenger) Survey(topic string, body []byte) ([][]byte, MessageId, error) {
	replies := future.NewFuture()
	msgr.peers.Send("survey", topic, body, replies)
	err := replies.Error()
	if err != nil {
		return nil, MsgId{}, err
	}

	var bodies [][]byte
	var msgId MessageId
	for _, reply := range replies.Value().([]future.Future) {
		replyMsg, _ := reply.Value().(*Message)
		if replyMsg != nil {
			bodies = append(bodies, replyMsg.Body)
			msgId = replyMsg.MessageId
		} else if err == nil {
			err = reply.Error()
		}
	}
	return bodies, msgId, err
}

func (msgr *messenger) Subscribe(topic string, handler Handler) {
	msgr.peers.Send("subscribe", topic, handler)
}

func (msgr *messenger) Unsubscribe(topic string) {
	msgr.peers.Send("unsubscribe", topic)
}
