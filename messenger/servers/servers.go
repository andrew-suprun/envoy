package servers

import (
	"bytes"
	"fmt"
	"github.com/andrew-suprun/envoy/actor"
	"github.com/andrew-suprun/envoy/future"
	. "github.com/andrew-suprun/envoy/messenger"
	. "github.com/andrew-suprun/envoy/messenger/common"
	"github.com/andrew-suprun/envoy/messenger/dialer"
	"github.com/andrew-suprun/envoy/messenger/reader"
	"github.com/andrew-suprun/envoy/messenger/writer"
	"math/rand"
	"net"
	"time"
)

type servers struct {
	actor.Actor
	hostId      HostId
	servers     map[HostId]*server
	dialer      Sender
	dialResult  future.Future
	leaveFuture future.Future
}

type server struct {
	serverId       HostId
	topics         map[Topic]struct{}
	pendingReplies map[MsgId]future.Future
	reader         Sender
	writer         Sender
	conn           net.Conn
	state          state
}

type state int

const (
	pendingDial state = iota
	pendingRedial
	dialed
	connected
)

func NewServers(hostId HostId, remotes []HostId, result future.Future) Sender {
	s := &servers{
		Actor:   actor.NewActor("clients-" + string(hostId)),
		hostId:  hostId,
		servers: make(map[HostId]*server),
	}

	s.dialer = dialer.NewDialer(s)

	s.Actor = actor.NewActor("servers-"+string(hostId)).
		RegisterHandler("dial", s.handleDial).
		RegisterHandler("dialed", s.handleDialed).
		RegisterHandler("dial-error", s.handleDialError).
		RegisterHandler("publish", s.handlePublish).
		RegisterHandler("message", s.handleMessage).
		RegisterHandler("leave", s.handleLeave).
		Start()

	if len(remotes) == 0 {
		result.SetValue(true)
		return s
	}

	s.dialResult = result

	for _, remote := range remotes {
		s.Send("dial", remote)
	}

	return s
}

func (s *servers) handleDial(_ string, info []interface{}) {
	remote, _ := info[0].(HostId)
	s.logf("handleDial: remote = %s", remote)
	if remote == s.hostId {
		return
	}
	if _, found := s.servers[remote]; found {
		return
	}

	s.servers[remote] = &server{
		serverId:       remote,
		topics:         make(map[Topic]struct{}),
		pendingReplies: make(map[MsgId]future.Future),
		state:          pendingDial,
	}
	s.dialer.Send("dial", remote)
}

func (s *servers) handleDialed(_ string, info []interface{}) {
	serverId := info[0].(HostId)
	conn := info[1].(net.Conn)
	s.logf("handleDialed: serverId = %s; conn = %s/%s", serverId, conn.LocalAddr(), conn.RemoteAddr())

	server, ok := s.servers[serverId]
	if !ok {
		Log.Errorf("Dialed to non-existing server: %s", serverId)
		return
	}

	if server.state != pendingDial {
		Log.Errorf("Dialed to server %s in wrong state: %s", serverId, server.state)
		return
	}

	server.conn = conn
	server.reader = reader.NewReader(fmt.Sprintf("servers-reader-%s-%s", s.hostId, serverId), serverId, conn, s)
	server.writer = writer.NewWriter(fmt.Sprintf("servers-writer-%s-%s", s.hostId, serverId), serverId, conn, s)

	joinMsg := &JoinMessage{HostId: s.hostId}
	for peer := range s.servers {
		s.logf("handleDialed: peer = %s", peer)
		joinMsg.Peers = append(joinMsg.Peers, peer)
	}
	buf := &bytes.Buffer{}
	Encode(joinMsg, buf)

	server.writer.Send("write", &Message{
		Body:        buf.Bytes(),
		MessageId:   NewId(),
		MessageType: Join,
	})
	server.state = dialed
}

func (s *servers) handleDialError(_ string, info []interface{}) {
	serverId := info[0].(HostId)
	server := s.servers[serverId]
	if server == nil {
		return
	}
	server.state = pendingRedial
	Log.Errorf("Failed to dial %s. Will re-dial.", serverId)
	time.AfterFunc(RedialInterval, func() {
		s.Send("dial", serverId)
	})
	s.setDialResult()
}

func (s *servers) handlePublish(_ string, info []interface{}) {
	msg := info[0].(*Message)
	reply := info[1].(future.Future)

	if s.leaveFuture != nil {
		reply.SetError(ServerDisconnectedError)
		return
	}

	server := s.selectTopicServer(msg.Topic)
	if server == nil {
		reply.SetError(NoSubscribersError)
		return
	}
	time.AfterFunc(Timeout, func() {
		s.Send("message", s.hostId, &Message{MessageId: msg.MessageId, MessageType: ReplyTimeout})
	})
	server.pendingReplies[msg.MessageId] = reply
	server.writer.Send("write", msg)
}

func (s *servers) selectTopicServer(t Topic) *server {
	servers := s.getServersByTopic(t)
	if len(servers) == 0 {
		return nil
	}
	return servers[rand.Intn(len(servers))]
}

func (s *servers) getServersByTopic(t Topic) []*server {
	result := []*server{}
	for _, server := range s.servers {
		if _, found := server.topics[t]; found {
			result = append(result, server)
		}
	}
	return result
}

func (s *servers) handleMessage(_ string, info []interface{}) {
	from := info[0].(HostId)
	msg := info[1].(*Message)

	server := s.servers[from]
	if server == nil {
		Log.Errorf("Received '%s' message from non-existing server %s. Ignored.", msg.MessageType, from)
		return
	}

	switch msg.MessageType {
	case Join:
		s.handleJoin(server, msg)
	case Reply:
		s.handleReply(server, msg)
	case ReplyTimeout:
		s.handleReplyError(server, msg, TimeoutError)
	case ReplyPanic:
		s.handleReplyError(server, msg, PanicError)
	case Subscribe:
		s.handleSubscribed(server, msg)
	case Unsubscribe:
		s.handleUnsubscribed(server, msg)
	case Left:
		s.handleLeft(server, msg)
	default:
		panic(fmt.Sprintf("received message: %v", msg))
	}
}

func (s *servers) handleJoin(server *server, msg *Message) {
	joinMessage := &JoinMessage{}
	Decode(bytes.NewBuffer(msg.Body), joinMessage)
	for _, topic := range joinMessage.Topics {
		server.topics[topic] = struct{}{}
	}
	server.state = connected
	s.logf("handleJoin: joinMessage = %v; peers = %d", joinMessage, len(joinMessage.Peers))
	for _, peer := range joinMessage.Peers {
		s.logf("handleJoin: dialing = %s", peer)
		s.Send("dial", peer)
	}
	s.setDialResult()
}

func (s *servers) setDialResult() {
	if s.dialResult == nil {
		return
	}
	for _, server := range s.servers {
		if server.state != connected && server.state != pendingRedial {
			return
		}
	}
	s.dialResult.SetValue(true)
}

func (s *servers) handleReply(server *server, msg *Message) {
	result := server.pendingReplies[msg.MessageId]
	delete(server.pendingReplies, msg.MessageId)
	if result == nil {
		Log.Errorf("Received unexpected reply for '%s'. Ignored.", msg.Topic)
		return
	}
	result.SetValue(msg)
}

func (s *servers) handleReplyError(server *server, msg *Message, err error) {
	result := server.pendingReplies[msg.MessageId]
	delete(server.pendingReplies, msg.MessageId)
	result.SetError(err)
}

func (s *servers) handleSubscribed(server *server, msg *Message) {
	buf := bytes.NewBuffer(msg.Body)
	var t Topic
	Decode(buf, &t)
	server.topics[t] = struct{}{}
}

func (s *servers) handleUnsubscribed(server *server, msg *Message) {
	buf := bytes.NewBuffer(msg.Body)
	var t Topic
	Decode(buf, &t)
	delete(server.topics, t)
}

func (s *servers) handleLeft(server *server, msg *Message) {
	panic("Implement me.")
	// if server == nil {
	// 	return
	// }

	// server.state = serverStopping
	// pendingFutures := make([]future.Future, len(server.pendingReplies))
	// for _, pf := range server.pendingReplies {
	// 	pendingFutures = append(pendingFutures, pf)
	// }
	// go stopPeer(server, pendingFutures, msgr)
}

func (s *servers) handleNetworkError(_ string, info []interface{}) {
	panic("Implement me.")
	// serverId := info[0].(hostId)
	// err := info[1].(error)
	// if s.state == messengerLeaving {
	// 	s.Send("shutdown-server", serverId)
	// 	return
	// }
	// if server, found := s.servers[serverId]; found {
	// 	if server.state == serverStopping || server.state == serverLeaving {
	// 		return
	// 	}
	// 	server.state = serverStopping
	// 	if err.Error() == "EOF" {
	// 		Log.Errorf("Peer %s disconnected. Will try to re-connect.", serverId)
	// 	} else {
	// 		Log.Errorf("Peer %s: Network error: %v. Will try to re-connect.", serverId, err)
	// 	}

	// 	s.Send("shutdown-server", server.serverId)

	// 	if serverId > s.hostId {
	// 		time.AfterFunc(time.Millisecond, func() {
	// 			s.Send("dial", serverId)
	// 		})
	// 	} else {
	// 		time.AfterFunc(RedialInterval, func() {
	// 			s.Send("dial", serverId)
	// 		})
	// 	}
	// }
}

func (s *servers) handleLeave(_ string, info []interface{}) {
	// panic("Implement me.")
	// s.leaveFuture = info[0].(future.Future)
	// if len(s.servers) == 0 {
	// 	s.leaveFuture.SetValue(true)
	// }
}

func (s *server) String() string {
	return fmt.Sprintf("server: state = %s", s.state)
}

func (s state) String() string {
	switch s {
	case pendingDial:
		return "pendingDial"
	case pendingRedial:
		return "pendingRedial"
	case dialed:
		return "dialed"
	case connected:
		return "connected"
	default:
		panic(fmt.Sprintf("Unknown server state = %d", s))
	}
}

func (s *servers) logf(format string, params ...interface{}) {
	Log.Debugf(">>> servers-%s: "+format, append([]interface{}{s.hostId}, params...)...)
}
