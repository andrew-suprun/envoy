package messenger

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ugorji/go/codec"
	mRand "math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultNetworkTimeout = time.Second
	DefaultPublishTimeout = 30 * time.Second
)

var (
	TimeoutError       = errors.New("timed out")
	NoSubscribersError = errors.New("no subscribers")
)

type PanicError struct {
	MessageId string
	At        *net.UDPAddr
	Stack     []byte
}

func (p *PanicError) Error() string {
	return fmt.Sprintf("Message %s panic-ed at %s", p.MessageId, p.At)
}

func NewMessenger() Messenger {
	return newMessenger()
}

type Messenger interface {
	Join(local string, remotes []string) (joined []string, err error)
	Leave()

	Publish(topic string, body []byte) ([]byte, error)
	Broadcast(topic string, body []byte) ([][]byte, error)

	// No more then one subscription per topic.
	// Second subscription panics.
	Subscribe(topic string, handler Handler) error
	Unsubscribe(topic string) error

	SetNetworkTimeout(timeout time.Duration)
	SetPublishTimeout(timeout time.Duration)
	SetLogger(logger Logger)
}

type Handler func(topic string, body []byte) []byte

//
// impl
//

const (
	pingRequest messageType = iota
	pongReply
	request
	ack
	reply
	joinRequest
	joinReply
	leaveMessage
	subscribeRequest
	subscribeReply
	unsubscribeRequest
	unsubscribeReply
)

const (
	active state = iota
	unresponsive
)

const (
	messageIdSize   = 16
	maxBodyPartSize = 8 * 1024
	bufferSize      = maxBodyPartSize + 8
)

type (
	topic           string
	hostId          string
	messageId       [messageIdSize]byte
	messageType     int
	handlers        map[topic]Handler
	hosts           map[hostId]*host
	pendingReplies  map[messageId]*pendingReply
	partialMessages map[messageId]*partialMessage
	state           int

	messenger struct {
		*net.UDPAddr
		*net.UDPConn
		Logger
		subscriptions
		peers
		networkTimeout time.Duration
		publishTimeout time.Duration

		// partialMessages are accessed from single readLoop goroutine
		// hence they need not be protected by mutex
		partialMessages
		partialMessagesHead *partialMessageNode
		partialMessagesTail *partialMessageNode
	}

	peers struct {
		sync.Mutex
		hosts
	}

	host struct {
		sync.Mutex
		hostId
		*net.UDPAddr
		state
		pendingReplies
		peers  map[hostId]state
		topics map[topic]struct{}
	}

	message struct {
		messageId
		messageType
		parts [][]byte
	}

	result struct {
		*host
		*header
		body []byte
		err  error
	}

	header struct {
		MessageId   messageId   `codec:"id"`
		MessageType messageType `codec:"mt"`
		Topic       string      `codec:"t,omitempty"`
		BodyLen     int         `codec:"bl,omitempty"`
		PartIndex   int         `codec:"pi,omitempty"`
		PartOffset  int         `codec:"po,omitempty"`
	}

	pendingReply struct {
		resultChan chan *result
	}

	partialMessage struct {
		time.Time
		results []result
	}

	partialMessageNode struct {
		*partialMessage
		next *partialMessageNode
	}

	joinRequestBody struct {
		Topics map[topic]struct{} `codec:"subs,omitempty"`
	}

	joinReplyBody struct {
		Topics map[topic]struct{} `codec:"subs,omitempty"`
		Peers  map[hostId]state   `codec:"hosts,omitempty"`
	}

	subscriptions struct {
		sync.Mutex
		handlers
	}
)

func withSubscriptions(msgr *messenger, f func(handlers)) {
	msgr.subscriptions.Lock()
	defer msgr.subscriptions.Unlock()
	f(msgr.subscriptions.handlers)
}

func withPeers(msgr *messenger, f func(hosts)) {
	msgr.peers.Mutex.Lock()
	defer msgr.peers.Unlock()
	f(msgr.peers.hosts)
}

func newHost(addr *net.UDPAddr) *host {
	return &host{
		hostId:         hostId(addr.String()),
		UDPAddr:        addr,
		pendingReplies: make(pendingReplies),
	}
}

func withHost(host *host, f func()) {
	host.Lock()
	defer host.Unlock()
	f()
}

func newMessenger() Messenger {
	return &messenger{
		networkTimeout:  DefaultNetworkTimeout,
		publishTimeout:  DefaultPublishTimeout,
		Logger:          defaultLogger,
		subscriptions:   subscriptions{handlers: make(handlers)},
		peers:           peers{hosts: make(hosts)},
		partialMessages: make(map[messageId]*partialMessage),
	}
}

func (msgr *messenger) Join(local string, remotes []string) (joined []string, err error) {
	msgr.UDPAddr, err = net.ResolveUDPAddr("udp", local)
	if err != nil {
		return nil, err
	}
	localHost := hostId(msgr.UDPAddr.String())

	msgr.UDPConn, err = net.ListenUDP("udp", msgr.UDPAddr)
	if err != nil {
		return nil, err
	}
	msgr.Info("Listening on: %s", local)

	go readLoop(msgr)

	if len(remotes) == 0 {
		return
	}

	topics := map[topic]struct{}{}
	withSubscriptions(msgr, func(h handlers) {
		for topic := range h {
			topics[topic] = struct{}{}
		}
	})

	buf := &bytes.Buffer{}
	codec.NewEncoder(buf, &ch).MustEncode(joinRequestBody{Topics: topics})
	joinMessageBody := buf.Bytes()

	////// new

	toJoinChan := make(chan hostId, len(remotes))
	sentInvitations := make(map[hostId]struct{})
	var (
		invitationMutex sync.Mutex
		sent            int64
		received        int64
	)

	for _, remote := range remotes {
		toJoinChan <- hostId(remote)
	}

	atomic.AddInt64(&sent, int64(len(remotes)))

	timeoutChan := time.After(msgr.networkTimeout)
	resultChan := make(chan *result)

	go func() {
		for {
			select {
			case result := <-resultChan:
				reply := &joinReplyBody{}
				decode(bytes.NewBuffer(result.body), reply)
				joined = append(joined, result.host.String())

				var fromHost *host
				withPeers(msgr, func(hosts hosts) {
					fromHost = hosts[result.hostId]
				})
				if fromHost == nil {
					msgr.Error("Received message from unknown host %s. Ignoring.", result.hostId)
					continue
				}
				fromHost.peers = reply.Peers
				fromHost.topics = reply.Topics

				withPeers(msgr, func(hosts hosts) {
					hosts[result.hostId] = fromHost
				})
				msgr.Info("Joined %s", result.hostId)
				for peer, state := range reply.Peers {
					_ = state // TODO: Handle peer state
					if peer != localHost {
						alreadyPresent := false
						withPeers(msgr, func(hosts hosts) {
							_, alreadyPresent = hosts[peer]
						})
						if !alreadyPresent {
							invitationMutex.Lock()
							_, alreadySent := sentInvitations[peer]
							invitationMutex.Unlock()

							if !alreadySent {
								atomic.AddInt64(&sent, 1)
								toJoinChan <- peer
							}
						}
					}
				}
				s := atomic.LoadInt64(&sent)
				r := atomic.AddInt64(&received, 1)
				if s == r {
					close(toJoinChan)
					return
				}
			case <-timeoutChan:
				err = TimeoutError
				close(toJoinChan)
				return
			}
		}
	}()

	for invitation := range toJoinChan {
		raddr, err := net.ResolveUDPAddr("udp", string(invitation))
		if err != nil {
			msgr.Info("Failed to resolve address %s (%v). Ignoring.", invitation, err)
			continue
		}
		invitation = hostId(raddr.String())

		invitationMutex.Lock()
		sentInvitations[hostId(invitation)] = struct{}{}
		invitationMutex.Unlock()

		requestId := newId()

		host := newHost(raddr)
		withPeers(msgr, func(hosts hosts) {
			hosts[hostId(invitation)] = host
		})

		go func() {
			body, err := sendMessage(msgr, newMessage("", joinMessageBody, requestId, joinRequest), host)
			resultChan <- &result{host: host, body: body, err: err}
		}()
	}

	return
}

func readLoop(msgr *messenger) {
	byteSlice := make([]byte, bufferSize)
	for {
		n, from, err := msgr.ReadFromUDP(byteSlice) // TODO: Shutdown on closed connection
		if err != nil {
			msgr.Logger.Error("Failed to read from UDP socket: %v", err)
			continue
		}
		if n == 0 {
			continue
		}
		now := time.Now()
		hId := hostId(from.String())
		buf := bytes.NewBuffer(byteSlice[:n])
		header := decodeHeader(buf)
		body := make([]byte, buf.Len())
		copy(body, buf.Bytes())
		msgr.Debug("<<< %s/%s(%d)", header.MessageId, header.MessageType, header.PartIndex)

		if header.MessageType != ack {
			ackn := newMessage("", nil, header.MessageId, ack)
			msgr.Debug(">1> %s/%s(%d)", header.MessageId, ack, header.PartIndex)
			n, err = msgr.WriteToUDP(ackn.parts[0], from)
		}

		var host *host
		withPeers(msgr, func(hosts hosts) {
			host = hosts[hId]
		})

		if host == nil {
			if header.MessageType == joinRequest {
				host = newHost(from)
				withPeers(msgr, func(hosts hosts) {
					hosts[hId] = host
				})
			} else {
				msgr.Error("Received message from unknown host %s. Ignoring.", from)
				continue
			}
		}

		res := &result{host: host, header: header, body: body}

		// collect all parts
		if header.BodyLen > len(body) {
			pMsg := msgr.partialMessages[header.MessageId]
			if pMsg == nil {
				pMsg = &partialMessage{Time: now, results: []result{result{host: host, header: header, body: body}}}
				msgr.partialMessages[header.MessageId] = pMsg
			} else {
				pMsg.results = append(pMsg.results, result{host: host, header: header, body: body})
			}
			res.body = joinParts(pMsg)
			if res.body == nil {
				continue
			}
		}

		switch header.MessageType {
		case request:
			go handleRequest(msgr, res)
		case ack, reply:
			go handleReply(msgr, res)
		case joinRequest:
			go handleJoinRequest(msgr, res)
		case joinReply:
			go handleJoinReply(msgr, res)
		default:
			panic(fmt.Errorf("Read unknown message type %s", header.MessageType))
		}

		expiration := now.Add(-msgr.networkTimeout)
		for msgr.partialMessagesHead != nil && msgr.partialMessagesHead.Time.Before(expiration) {
			delete(msgr.partialMessages, msgr.partialMessagesHead.partialMessage.results[0].MessageId)
			msgr.partialMessagesHead = msgr.partialMessagesHead.next
		}
	}
}

func joinParts(pm *partialMessage) []byte {
	// all results are already sorted here with possible exception of the last one

	lastIndex := len(pm.results) - 1
	index := lastIndex
	lastOffset := pm.results[lastIndex].PartOffset
	for index > 0 && pm.results[index-1].PartOffset > lastOffset {
		index--
	}
	if index < lastIndex {
		if pm.results[index].PartOffset == lastOffset {
			// ignore duplicate part
			pm.results = pm.results[:lastIndex]
		} else {
			lastResult := pm.results[lastIndex]
			copy(pm.results[index+1:], pm.results[index:])
			pm.results[index] = lastResult
		}
	}

	offset := 0
	// for index := 0; index <= lastIndex; index++ {
	for _, result := range pm.results {
		if result.PartOffset != offset {
			return nil
		}
		offset += len(result.body)
	}

	bodyLen := pm.results[0].BodyLen

	if offset != bodyLen {
		return nil
	}

	body := make([]byte, bodyLen)
	for _, result := range pm.results {
		copy(body[result.PartOffset:], result.body)
	}

	return body
}

func handleRequest(msgr *messenger, res *result) {
	handler, found := Handler(nil), false
	withSubscriptions(msgr, func(handlers handlers) {
		handler, found = handlers[topic(res.Topic)]
	})

	if !found {
		msgr.Info("Received request for non-subscribed topic %s. Ignored.", res.Topic)
		return
	}

	go func() {
		result := handler(res.Topic, res.body)
		sendMessage(msgr, newMessage("", result, res.MessageId, reply), res.host)
	}()
}

func handleReply(msgr *messenger, res *result) {
	host, ok := (*host)(nil), false
	withPeers(msgr, func(hosts hosts) {
		host, ok = hosts[res.hostId]
	})
	if !ok {
		msgr.Logger.Error("Received reply from unknown peer %s. Ignoring.", res.hostId)
		return
	}

	pending, found := (*pendingReply)(nil), false
	withHost(host, func() {
		pending, found = host.pendingReplies[res.MessageId]
	})

	if found {
		pending.resultChan <- res
		// } else {
		// log.Printf("~~~ Received unexpected reply[%T]: %s", res, res)
		// log.Printf("~~~ messageId = %s\n%s", res.MessageId)
		// os.Exit(1)
	}
}

func handleJoinRequest(msgr *messenger, res *result) {
	reply := &joinReplyBody{
		Topics: map[topic]struct{}{},
		Peers:  make(map[hostId]state),
	}

	withSubscriptions(msgr, func(handlers handlers) {
		for topic := range handlers {
			reply.Topics[topic] = struct{}{}
		}
	})

	withPeers(msgr, func(hosts hosts) {
		for peer, host := range hosts {
			reply.Peers[peer] = host.state
		}
	})

	buf := &bytes.Buffer{}
	encode(reply, buf)

	_, err := sendMessage(msgr, newMessage("", buf.Bytes(), res.MessageId, joinReply), res.host)
	if err != nil {
		msgr.Info("Failed to send join reply message to %s: %s", res.hostId, err.Error())
		return
	}

	buf = bytes.NewBuffer(res.body)
	request := &joinRequestBody{}
	decode(buf, request)

	withHost(res.host, func() {
		res.topics = request.Topics
	})

	msgr.Info("Joined %s", res.hostId)
}

func handleJoinReply(msgr *messenger, res *result) {
	buf := bytes.NewBuffer(res.body)
	reply := &joinReplyBody{}
	decode(buf, reply)

	var peer *host
	withPeers(msgr, func(hosts hosts) {
		peer = hosts[hostId(res.hostId)]
	})

	if peer == nil {
		msgr.Error("Received joinReply from unexpected host '%s'. Ignoring.", res.hostId)
		return
	}

	pr, prFound := (*pendingReply)(nil), false
	withHost(peer, func() {
		pr, prFound = peer.pendingReplies[res.MessageId]
	})

	if prFound {
		pr.resultChan <- res
	} else {
		msgr.Error("There is no message waiting for joinReply from %s", res.hostId)
	}
}

func (msgr *messenger) Leave() {
	// TODO
}

func (msgr *messenger) Subscribe(_topic string, handler Handler) error {
	withSubscriptions(msgr, func(handlers handlers) {
		handlers[topic(_topic)] = handler
	})

	if msgr.UDPConn != nil {
		// TODO: broadcast subscribe message
	}
	return nil
}

func (msgr *messenger) Unsubscribe(_topic string) error {
	withSubscriptions(msgr, func(handlers handlers) {
		delete(handlers, topic(_topic))
	})

	if msgr.UDPConn != nil {
		// TODO: broadcast unsubscribe message
	}
	return nil
}

func (msgr *messenger) Publish(_topic string, body []byte) ([]byte, error) {

	to := selectHost(msgr, topic(_topic))
	if to == nil {
		return []byte{}, NoSubscribersError
	}

	msgId := newId()

	// TODO: Handle timeouts
	return sendMessage(msgr, newMessage(_topic, body, msgId, request), to)
}

func newMessage(topic string, body []byte, msgId messageId, msgType messageType) *message {
	bodyLen := len(body)
	partIndex := 0
	partOffset := 0
	parts := make([][]byte, 0, bodyLen/(maxBodyPartSize+40)+1)

	for partOffset <= bodyLen {
		msgHeader := header{
			Topic:       topic,
			MessageId:   msgId,
			MessageType: msgType,
			BodyLen:     bodyLen,
			PartIndex:   partIndex,
			PartOffset:  partOffset,
		}

		buf := bytes.Buffer{}
		enc := codec.NewEncoder(&buf, &ch)
		enc.MustEncode(msgHeader)

		if partOffset == bodyLen {
			parts = append(parts, buf.Bytes())
			break
		}

		capacity := maxBodyPartSize - buf.Len()
		remaining := bodyLen - partOffset
		if capacity >= remaining {
			buf.Write(body[partOffset:])
			parts = append(parts, buf.Bytes())
			break
		} else {
			buf.Write(body[partOffset : partOffset+capacity])
			parts = append(parts, buf.Bytes())
			partOffset += capacity
			partIndex++
		}
	}
	return &message{
		messageId:   msgId,
		messageType: msgType,
		parts:       parts,
	}
}

func sendMessage(msgr *messenger, parts *message, to *host) ([]byte, error) {
	resultChan := make(chan *result)

	withHost(to, func() {
		to.pendingReplies[parts.messageId] = &pendingReply{
			resultChan: resultChan,
		}
	})

	// var timeoutChan <-chan time.Time
	// if isSync(parts.messageType) {
	// 	timeoutChan = time.After(msgr.publishTimeout)
	// } else {
	// 	timeoutChan = time.After(msgr.networkTimeout)
	// }
	timeoutChan := time.After(msgr.publishTimeout)

	acked := make([]bool, len(parts.parts))
	timeout := 100 * time.Millisecond

	for i, part := range parts.parts {
		msgr.Debug(">2> %s/%s(%d)", parts.messageId, parts.messageType, i)
		_, err := msgr.WriteToUDP(part, to.UDPAddr)
		if err != nil {
			return nil, err
		}
	}

	for {
		select {
		case result := <-resultChan:
			if result.header.MessageType == ack {
				acked[result.header.PartIndex] = true
				if !isSync(parts.messageType) && allAcked(acked) {
					return nil, nil
				}
			} else {
				withHost(to, func() {
					delete(to.pendingReplies, parts.messageId)
				})
				return result.body, nil
			}
		case <-time.After(timeout):
			for i, part := range parts.parts {
				if !acked[i] {
					msgr.Debug(">3> %s/%s(%d) timeout: %s", parts.messageId, parts.messageType, i, timeout)
					_, err := msgr.WriteToUDP(part, to.UDPAddr)
					if err != nil {
						return nil, err
					}
				}
			}
			timeout = timeout * 2
		case <-timeoutChan:
			msgr.Debug(">4> %s/%s(%d) timeout: %s", parts.messageId, parts.messageType, 0, timeout)
			withHost(to, func() {
				to.state = unresponsive
			})
			return nil, TimeoutError
		}
	}
}

func isSync(msgType messageType) bool {
	return msgType == request || msgType == subscribeRequest || msgType == joinRequest
}

func allAcked(acked []bool) bool {
	for _, a := range acked {
		if !a {
			return false
		}
	}
	return true
}

func selectHost(msgr *messenger, t topic) (peer *host) {
	withPeers(msgr, func(hosts hosts) {
		subscribers := ([]*host)(nil)
		unresponsive := ([]*host)(nil)
		for _, host := range hosts {
			if _, ok := host.topics[t]; ok {
				if host.state == active {
					subscribers = append(subscribers, host)
				} else {
					subscribers = append(subscribers, host)
				}
			}
		}
		if len(subscribers) == 0 {
			subscribers = unresponsive
		}
		if len(subscribers) == 0 {
			return
		}

		peer = subscribers[mRand.Intn(len(subscribers))]
		return
	})
	return peer
}

func (msgr *messenger) Broadcast(topic string, body []byte) ([][]byte, error) {
	return nil, nil
}

func (msgr *messenger) SetNetworkTimeout(timeout time.Duration) {
	msgr.networkTimeout = timeout
}

func (msgr *messenger) SetPublishTimeout(timeout time.Duration) {
	msgr.publishTimeout = timeout
}

func (msgr *messenger) SetLogger(logger Logger) {
	msgr.Logger = logger
}

func newHeader(topic string, msgType messageType) *header {
	return &header{
		MessageId:   newId(),
		MessageType: msgType,
		Topic:       topic,
	}
}

var ch codec.CborHandle

func encode(v interface{}, buf *bytes.Buffer) {
	codec.NewEncoder(buf, &ch).MustEncode(v)
}

func decode(buf *bytes.Buffer, v interface{}) {
	dec := codec.NewDecoder(buf, &ch)
	dec.MustDecode(v)
}

func decodeHeader(buf *bytes.Buffer) *header {
	msgHeader := &header{}
	decode(buf, msgHeader)
	return msgHeader
}

func newId() (mId messageId) {
	rand.Read(mId[:])
	return
}

func (mId messageId) String() string {
	return hex.EncodeToString(mId[:])
}

func (mType messageType) String() string {
	switch mType {
	case pingRequest:
		return "pingRequest"
	case pongReply:
		return "pongReply"
	case request:
		return "request"
	case ack:
		return "ack"
	case reply:
		return "reply"
	case joinRequest:
		return "joinRequest"
	case joinReply:
		return "joinReply"
	case leaveMessage:
		return "leaveMessage"
	case subscribeRequest:
		return "subscribeRequest"
	case subscribeReply:
		return "subscribeReply"
	case unsubscribeRequest:
		return "unsubscribeRequest"
	case unsubscribeReply:
		return "unsubscribeReply"
	default:
		panic(fmt.Errorf("Unknown messageType %d", mType))
	}
}

func (s state) String() string {
	switch s {
	case active:
		return "active"
	case unresponsive:
		return "unresponsive"
	default:
		return "unknown"
	}
}

func (h *host) String() string {
	return fmt.Sprintf("[host: id: %s; state %s; topics %d; peers %d; pendingReplies %d; ]", h.hostId, h.state,
		len(h.topics), len(h.peers), len(h.pendingReplies))
}

func (res *result) String() string {
	if res == nil {
		return "[result: nil]"
	}
	return fmt.Sprintf("[result: from: %s; %s; body %d]", res.host, res.header, len(res.body))
}

func (h *header) String() string {
	return fmt.Sprintf("[header: messageId: '%s'; messageType '%s'; topic: '%s']", h.MessageId, h.MessageType, h.Topic)
}
