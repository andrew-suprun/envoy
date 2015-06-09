package messenger

import (
	"github.com/andrew-suprun/envoy/actor"
	"net"
)

type server struct {
	actor.Actor
	hostId
	conn net.Conn
	msgr actor.Actor
	pendingReplies
}

func newServer(serverId hostId, msgr actor.Actor) actor.Actor {
	srv := &server{
		hostId:         serverId,
		Actor:          actor.NewActor("server"),
		pendingReplies: make(pendingReplies),
		in:             make(chan serverMessage),
		msgr:           msgr,
	}
	return srv.
		RegisterHandler("message", srv.handleMessage).
		RegisterHandler("subscribe", srv.handleSubscribe).
		RegisterHandler("un-subscribe", srv.handleUnubscribe).
		RegisterHandler("publish", srv.handlePublish).
		RegisterHandler("reply", srv.handleReply).
		RegisterHandler("stop", srv.handleStop).
		Start()
}

func (srv *server) handleStart(_ actor.MessageType, _ actor.Payload) {
	srv.dial()
	go srv.readLoop()
}

func (srv *server) readLoop() {
	for {
		msg, err := readMessage(srv.conn)
		if err != nil {
			msgr.Send("server-gone", srv.hostId)
			if err.Error() == "EOF" {
				Log.Debugf("%s: Client %s disconnected.", msgr.hostId, clientId)
			} else {
				Log.Errorf("%s: Failed to read from client. Disconnecting.", msgr.hostId)
			}
			srv.stop()
			return
		}

		switch msg.MessageType {
		case publish, request:
			go msgr.handleRequest(clientId, msg)
		default:
			panic(fmt.Errorf("Read unknown client message type %s", msg.MessageType))
		}
	}
}

func (srv *server) dial() {
	for !srv.stopped {
		conn, err := net.Dial("tcp", string(serverId))
		if err != nil {
			log.Errorf("%s: Failed to connect to '%s'. Will re-try.", msgr.hostId, serverId)
			time.Sleep(RedialInterval)
			continue
		}

		err = writeJoinInvite(conn)
		if err != nil {
			log.Errorf("%s: Failed to invite '%s'. Will re-try.", msgr.hostId, serverId)
			time.Sleep(RedialInterval)
			continue
		}

		reply, err := readJoinAccept(conn)
		if err != nil {
			log.Errorf("Failed to read join accept from '%s'. Will re-try.", conn)
			time.Sleep(RedialInterval)
			continue
		}

		srv.conn = conn
		srv.msgr.Send("got-peers", reply.Peers)
		return
	}
}

func (srv *server) message(msg *message, err error) {
	if err != nil {
		go srv.msgr.readError(srv.hostId, err)
		return
	}
	switch msg.MessageType {
	case reply, replyPanic:
		srv.handleReply(msg)
	case subscribe, unsubscribe:
		srv.handleSubscription(msg)
	case leaving:
		srv.handleLeaving()
	default:
		panic(fmt.Errorf("Read unknown client message type %s", msg.MessageType))
	}
}

func (srv *server) hasTopic(_topic topic) bool {
	respChan := make(chan bool)
	srv.in <- hasTopicMessage{topic: _topic, responseChan: respChan}
	return <-respChan
}

func (srv *server) handleLeaving(serverId hostId) {
	srv.stop()
	go srv.msgr.serverGone(srv.hostId)
}

func (srv *server) handleReply(serverId hostId, msg *message) {
	pending := srv.pendingReplies[msg.MessageId]
	if pending == nil {
		Log.Errorf("Received unexpected message '%s'. Ignored.", msg.MessageType)
		return
	}

	if msg.MessageType == replyPanic {
		pending <- &actorMessage{message: msg, error: PanicError}
	} else {
		pending <- &actorMessage{message: msg}
	}
}

func (srv *server) handleSubscription(serverId hostId, msg *message) {

	buf := bytes.NewBuffer(msg.Body)
	var _topic topic
	decode(buf, &_topic)

	switch msg.MessageType {
	case subscribe:
		msgr.addServerTopic(serverId, _topic)
	case unsubscribe:
		msgr.removeServerTopic(serverId, _topic)
	default:
		panic("Wrong message type for handleSubscription")
	}
}

func (srv *server) leave() {

	srv.wg.Wait()

}

func (srv *server) shutdown() {
	// todo
	if server != nil {
		for _, replyChan := range server.pendingReplies {
			replyChan <- &actorMessage{error: ServerDisconnectedError}
		}
	}
	if server.conn != nil {
		server.conn.Close()
	}
}

func sliceTopics(topicMap map[topic]struct{}) []topic {
	result := make([]topic, len(topicMap))
	for topic := range topicMap {
		result = append(result, topic)
	}
	return result
}

func mapTopics(topicSlice []topic) map[topic]struct{} {
	result := make(map[topic]struct{}, len(topicSlice))
	for _, topic := range topicSlice {
		result[topic] = struct{}{}
	}
	return result
}

func (srv *server) stop() {
	close(srv.in)
}

func writeJoinInvite(conn net.Conn) error {
	buf := &bytes.Buffer{}
	encode(msgr.hostId, buf)
	return writeMessage(conn, &message{MessageId: newId(), MessageType: joinInvite, Body: buf.Bytes()})
}

func readJoinAccept(conn net.Conn) (*joinAcceptBody, error) {
	joinMsg, err := readMessage(conn)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(joinMsg.Body)
	reply := &joinAcceptBody{}
	decode(buf, reply)
	return reply, nil
}
