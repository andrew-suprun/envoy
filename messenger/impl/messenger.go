package impl

import (
	"fmt"
	"github.com/andrew-suprun/envoy/future"
	m "github.com/andrew-suprun/envoy/messenger"
	"github.com/andrew-suprun/envoy/messenger/clients"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"github.com/andrew-suprun/envoy/messenger/servers"
	"net"
	"runtime/debug"
)

type messenger struct {
	addr    string
	servers Sender
	clients Sender
}

func init() {
	m.NewMessenger = func(local string) m.Messenger { return &messenger{addr: local} }
}

func (msgr *messenger) Join(remotes ...string) error {
	localAddr, err := resolveAddr(msgr.addr)
	if err != nil {
		return err
	}

	resolved := make([]HostId, 0, len(remotes))
	for _, remote := range remotes {
		remoteAddr, err := resolveAddr(remote)
		if err == nil {
			resolved = append(resolved, remoteAddr)
		} else {
			m.Log.Errorf("Cannot resolve address %s. Ignoring.", remote)
		}
	}

	result := future.NewFuture()
	msgr.servers = servers.NewServers(localAddr, resolved, result)
	if result.Error() != nil {
		return err
	}

	msgr.clients, err = clients.NewClients(localAddr, msgr.servers)
	if err != nil {
		msgr.servers.Send("leave")
		return err
	}

	return nil
}

func (msgr *messenger) Leave() {
	m.Log.Debugf("msgr:Leave: stack:\n%s", string(debug.Stack()))
	f := futures(2)

	msgr.clients.Send("leave", f[0])
	msgr.servers.Send("leave", f[1])

	waitFutures(f)
}

func (msgr *messenger) Publish(topic string, body []byte) (m.MessageId, error) {
	msg := NewMessage(Topic(topic), body, Publish)
	result := future.NewFuture()
	msgr.servers.Send("publish", msg, result)

	if err := result.Error(); err != nil {
		return msg.MessageId, err
	}

	return msg.MessageId, nil
}

func (msgr *messenger) Request(topic string, body []byte) ([]byte, m.MessageId, error) {
	msg := NewMessage(Topic(topic), body, Request)
	result := future.NewFuture()
	msgr.servers.Send("publish", msg, result)

	if err := result.Error(); err != nil {
		return nil, msg.MessageId, err
	}

	resultBody := result.Value().([]byte)

	return resultBody, msg.MessageId, nil
}

func (msgr *messenger) Broadcast(topic string, body []byte) (m.MessageId, error) {
	msg := NewMessage(Topic(topic), body, Publish)
	result := future.NewFuture()
	msgr.servers.Send("broadcast", msg, result)

	if err := result.Error(); err != nil {
		return msg.MessageId, err
	}

	return msg.MessageId, nil
}

func (msgr *messenger) Survey(topic string, body []byte) ([][]byte, m.MessageId, error) {
	msg := NewMessage(Topic(topic), body, Request)
	result := future.NewFuture()
	msgr.servers.Send("broadcast", msg, result)

	if err := result.Error(); err != nil {
		return nil, msg.MessageId, err
	}

	resultBodies := result.Value().([][]byte)

	return resultBodies, msg.MessageId, nil
}

func (msgr *messenger) Subscribe(topic string, handler m.Handler) {
	msgr.clients.Send("subscribe", topic, handler)
}

func (msgr *messenger) Unsubscribe(topic string) {
	msgr.clients.Send("unsubscribe", topic)
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

// func stopPeer(peer *peer, pendingFutures []future.Future, msgr actor.Actor) {
// 	for _, pf := range pendingFutures {
// 		if pf != nil {
// 			pf.Value()
// 		}
// 	}
// 	msgr.Send("shutdown-peer", peer.peerId)
// 	if peer.state == peerLeaving {
// 		Log.Infof("Peer %s left.", peer.peerId)
// 	}
// 	peer.writer.Send("write", &message{MessageType: left})

// }

// func (msgr *messenger) handleWriteResult(_ string, info []interface{}) {
// 	peerId := info[0].(hostId)
// 	msg := info[1].(*message)
// 	var err error
// 	if len(info) > 2 && info[2] != nil {
// 		err = info[2].(error)
// 	}

// 	peer := msgr.peers[peerId]
// 	if peer != nil {
// 		if err != nil {
// 			msgr.Send("shutdown-peer", peer.peerId)
// 			if peerId > msgr.hostId {
// 				msgr.Send("dial", peerId)
// 			}
// 		}

// 		if msg.MessageType == publish || msg.MessageType == subscribe || msg.MessageType == unsubscribe {
// 			if reply, exists := peer.pendingReplies[msg.MsgId]; exists {
// 				delete(peer.pendingReplies, msg.MsgId)
// 				reply.SetError(err)
// 			}
// 		}
// 	}
// }

// func (msgr *messenger) getTopics() []Topic {
// 	topics := make([]Topic, 0, len(msgr.subscriptions))
// 	for topic := range msgr.subscriptions {
// 		topics = append(topics, topic)
// 	}
// 	return topics
// }

// func (msgr *messenger) handleSendMessage(_ string, info []interface{}) {
// 	msg := info[0].(*message)
// 	reply := info[1].(future.Future)

// 	server := msgr.selectTopicServer(msg.Topic)
// 	if server == nil {
// 		reply.SetError(NoSubscribersError)
// 		return
// 	}
// 	server.pendingReplies[msg.MsgId] = reply
// 	server.writer.Send("write", msg)
// }

// func (msgr *messenger) selectTopicServer(t Topic) *peer {
// 	servers := msgr.getServersByTopic(t)
// 	if len(servers) == 0 {
// 		return nil
// 	}
// 	return servers[mRand.Intn(len(servers))]
// }

// func (msgr *messenger) getServersByTopic(t Topic) []*peer {
// 	result := []*peer{}
// 	for _, server := range msgr.peers {
// 		if server.state == peerConnected {
// 			if _, found := server.topics[t]; found {
// 				result = append(result, server)
// 			}
// 		}
// 	}
// 	return result
// }

// func (msgr *messenger) connId(conn net.Conn) string {
// 	if conn == nil {
// 		return fmt.Sprintf("%s/<nil>", msgr.hostId)
// 	}
// 	return fmt.Sprintf("%s/%s->%s", msgr.hostId, conn.LocalAddr(), conn.RemoteAddr())
// }

// func (peer *peer) String() string {
// 	return fmt.Sprintf("[peer: id: %s; topics: %d; state: %s]", peer.peerId, len(peer.topics), peer.state)
// }

// func (s peerState) String() string {
// 	switch s {
// 	case peerInitial:
// 		return "initial"
// 	case peerConnected:
// 		return "connected"
// 	case peerStopping:
// 		return "stopping"
// 	case peerLeaving:
// 		return "leaving"
// 	default:
// 		panic(fmt.Errorf("Unknown peerState %d", s))
// 	}
// }

// func (msgr *messenger) newJoinMessage() *JoinMessage {
// 	joinMsg := &JoinMessage{HostId: msgr.hostId}
// 	for t := range msgr.subscriptions {
// 		joinMsg.Topics = append(joinMsg.Topics, t)
// 	}
// 	for p := range msgr.peers {
// 		joinMsg.Peers = append(joinMsg.Peers, p)
// 	}

// 	return joinMsg
// }
