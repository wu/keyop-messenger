package federation

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/tlsutil"
)

// logger is a minimal logging interface used by federation components.
type logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

const wireVersion = "1"

// peerEntry tracks an active peer connection on the hub.
type peerEntry struct {
	name                  string
	sender                *PeerSender   // may be nil if this peer only sends to us
	receiver              *PeerReceiver // may be nil if we only send to this peer
	policy                *AtomicPolicy
	conn                  *websocket.Conn
	connWriteMu           *sync.Mutex // protects concurrent writes to conn (from sender and receiver)
	effectiveSubscription []string    // channels to forward to this peer (intersection of request and allowlist)
}

// Hub accepts incoming WebSocket connections and enforces the allowlist and receive policy.
type Hub struct {
	instanceName  string
	tlsCfg        *tls.Config
	localWriter   func(*envelope.Envelope) error
	dedup         Deduplicator
	auditL        audit.AuditLogger
	log           logger
	sendBufSize   int
	maxBatchBytes int
	dataDir       string

	mu                 sync.RWMutex
	cfg                HubConfig
	peers              map[string]*peerEntry   // keyed by instance name
	channelSubscribers map[string][]*peerEntry // channel -> list of subscribed peers (reverse index)
	listener           net.Listener
	httpSrv            *http.Server

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewHub constructs a Hub. Call Listen to start accepting connections.
func NewHub(
	instanceName string,
	cfg HubConfig,
	tlsCfg *tls.Config,
	localWriter func(*envelope.Envelope) error,
	dedup Deduplicator,
	auditL audit.AuditLogger,
	log logger,
	sendBufSize, maxBatchBytes int,
	dataDir string,
) *Hub {
	return &Hub{
		instanceName:       instanceName,
		cfg:                cfg,
		tlsCfg:             tlsCfg,
		localWriter:        localWriter,
		dedup:              dedup,
		auditL:             auditL,
		log:                log,
		sendBufSize:        sendBufSize,
		maxBatchBytes:      maxBatchBytes,
		dataDir:            dataDir,
		peers:              make(map[string]*peerEntry),
		channelSubscribers: make(map[string][]*peerEntry),
		stop:               make(chan struct{}),
	}
}

var hubUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

// Listen starts a TLS-wrapped HTTP/WebSocket listener on addr and returns
// immediately; connections are served in background goroutines.
func (h *Hub) Listen(addr string) error {
	ln, err := tls.Listen("tcp", addr, h.tlsCfg)
	if err != nil {
		return fmt.Errorf("federation: hub listen %s: %w", addr, err)
	}
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := hubUpgrader.Upgrade(w, r, nil)
			if err != nil {
				h.log.Error("federation: hub ws upgrade", "err", err)
				return
			}
			tlsState := r.TLS
			h.serveConn(conn, tlsState)
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}
	h.mu.Lock()
	h.listener = ln
	h.httpSrv = srv
	h.mu.Unlock()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		_ = srv.Serve(ln)
	}()
	return nil
}

// ServeTestConn is the same as the internal serveConn; exported so tests can
// inject pre-dialed websocket connections without a real TLS listener.
func (h *Hub) ServeTestConn(conn *websocket.Conn, tlsState *tls.ConnectionState) {
	go h.serveConn(conn, tlsState)
}

// Addr returns the listener address, or empty string if not listening.
func (h *Hub) Addr() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.listener == nil {
		return ""
	}
	return h.listener.Addr().String()
}

// EnqueueToAll enqueues env to every peer sender that is subscribed to the channel.
// Only peers that explicitly subscribed to this channel will receive the message.
// All peers have senders created at connection time to support forwarding.
func (h *Hub) EnqueueToAll(env *envelope.Envelope) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, entry := range h.peers {
		// Only forward to peers subscribed to this channel (using effective subscription after hub's allowlist)
		isSubscribed := false
		for _, ch := range entry.effectiveSubscription {
			if ch == env.Channel {
				isSubscribed = true
				break
			}
		}
		if !isSubscribed {
			continue
		}

		if entry.sender != nil {
			entry.sender.Enqueue(env)
		}
	}
}

// ReplayFrom scans channel files under dataDir/channels for envelopes on any
// of the listed channels, starting from (but not including) the record whose ID
// equals lastID. If lastID is empty or not found, replay starts from the
// earliest available record and logs EventReplayGap.
func (h *Hub) ReplayFrom(lastID string, channels []string) <-chan *envelope.Envelope {
	out := make(chan *envelope.Envelope, 64)
	go func() {
		defer close(out)
		channelSet := make(map[string]bool, len(channels))
		for _, ch := range channels {
			channelSet[ch] = true
		}

		// Collect all (channel, segment) pairs in offset order.
		type seg struct {
			channelDir string
			path       string
		}
		var segs []seg
		for _, ch := range channels {
			dir := filepath.Join(h.dataDir, "channels", ch)
			entries, err := os.ReadDir(dir)
			if err != nil {
				continue
			}
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				segs = append(segs, seg{
					channelDir: dir,
					path:       filepath.Join(dir, e.Name()),
				})
			}
		}

		// Scan all segments to find lastID, then emit from there.
		found := lastID == ""
		if !found {
			// First pass: locate lastID.
		outer:
			for _, s := range segs {
				f, err := os.Open(s.path)
				if err != nil {
					continue
				}
				sc := bufio.NewScanner(f)
				for sc.Scan() {
					env, err := envelope.Unmarshal(sc.Bytes())
					if err != nil {
						continue
					}
					if env.ID == lastID {
						found = true
						_ = f.Close()
						break outer
					}
				}
				_ = f.Close()
			}
		}

		if !found {
			h.log.Warn("federation: replay gap: lastID not in local files",
				"last_id", lastID)
			_ = h.auditL.Log(audit.Event{
				Event:     audit.EventReplayGap,
				MessageID: lastID,
			})
		}

		// Second pass: emit all envelopes after lastID (or all if gap/empty).
		pastLastID := lastID == "" || !found

		for _, s := range segs {
			f, err := os.Open(s.path)
			if err != nil {
				continue
			}
			sc := bufio.NewScanner(f)
			for sc.Scan() {
				env, err := envelope.Unmarshal(sc.Bytes())
				if err != nil {
					continue
				}
				if !pastLastID {
					if env.ID == lastID {
						pastLastID = true
					}
					continue
				}
				if !channelSet[env.Channel] {
					continue
				}
				envCopy := env
				select {
				case out <- &envCopy:
				case <-h.stop:
					_ = f.Close()
					return
				}
			}
			_ = f.Close()
		}
	}()
	return out
}

// Close stops the listener and all active peer connections.
func (h *Hub) Close() error {
	close(h.stop)
	h.mu.Lock()
	if h.httpSrv != nil {
		_ = h.httpSrv.Close()
	}
	for _, entry := range h.peers {
		_ = entry.conn.Close()
	}
	h.mu.Unlock()
	h.wg.Wait()
	return nil
}

// serveConn handles one incoming WebSocket connection: receives the peer
// handshake, checks the allowlist, sends the hub handshake if allowed, then
// starts the receiver (and optionally sender) goroutines.
func (h *Hub) serveConn(conn *websocket.Conn, tlsState *tls.ConnectionState) {
	// Receive peer handshake first so we know its identity.
	hs, err := ReceiveHandshake(conn)
	if err != nil {
		h.log.Error("federation: hub receive handshake", "err", err)
		_ = conn.Close()
		return
	}

	// Extract instance name: prefer TLS CN (tamper-proof) over handshake field.
	peerName := hs.InstanceName
	if tlsState != nil && len(tlsState.PeerCertificates) > 0 {
		peerName = tlsutil.ExtractCN(tlsState.PeerCertificates[0])
	}

	// Allowlist check before sending any response.
	h.mu.RLock()
	cfg := h.cfg
	h.mu.RUnlock()

	allowed := cfg.IsPeerAllowed(peerName)

	if !allowed {
		_ = conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(4403, "not in allowlist"))
		_ = conn.Close()
		_ = h.auditL.Log(audit.Event{Event: audit.EventClientRejected, Peer: peerName})
		h.log.Warn("federation: hub rejected connection", "peer", peerName)
		return
	}

	// Send hub handshake.
	if err := SendHandshake(conn, HandshakeMsg{
		InstanceName: h.instanceName,
		Role:         "hub",
		Version:      wireVersion,
	}); err != nil {
		h.log.Error("federation: hub send handshake", "err", err)
		_ = conn.Close()
		return
	}

	// Compute the effective channels this hub will send to the peer:
	// intersection of the peer's Subscribe request and the hub's allowlist.
	subChannels := effectiveSubscribeChannels(hs.Subscribe, peerName, cfg)
	pubChannels := publishChannelsFor(peerName, cfg)
	policy := NewAtomicPolicy(ForwardPolicy{
		Forward: subChannels,
		Receive: pubChannels,
	})

	// Start receiver. Also start a sender for all peers (even those without subscriptions)
	// so that messages from other peers can be forwarded to them. When both share a conn,
	// route acks via an internal channel so the PeerReceiver owns all reads and avoids
	// a concurrent-read race.
	needSender := true

	var receiver *PeerReceiver
	var sender *PeerSender
	connWriteMu := &sync.Mutex{} // shared mutex for protecting concurrent writes to conn

	if needSender {
		ackCh := make(chan AckMsg, 4)
		receiver = newPeerReceiverWithAck(conn, connWriteMu, policy, h.dedup, h.localWriter,
			h.auditL, h.log, peerName, h.maxBatchBytes, ackCh)
		sender = newPeerSenderWithAck(conn, connWriteMu, h.sendBufSize, h.maxBatchBytes, h.log, ackCh)
	} else {
		receiver = NewPeerReceiver(conn, connWriteMu, policy, h.dedup, h.localWriter,
			h.auditL, h.log, peerName, h.maxBatchBytes)
	}

	entry := &peerEntry{
		name:                  peerName,
		sender:                sender,
		receiver:              receiver,
		policy:                policy,
		conn:                  conn,
		connWriteMu:           connWriteMu,
		effectiveSubscription: subChannels,
	}

	h.mu.Lock()
	h.peers[peerName] = entry
	// Also add to reverse index by channel for fast EnqueueToAll lookups.
	for _, ch := range subChannels {
		h.channelSubscribers[ch] = append(h.channelSubscribers[ch], entry)
	}
	h.mu.Unlock()

	_ = h.auditL.Log(audit.Event{Event: audit.EventClientConnected, Peer: peerName})
	if hs.Ephemeral {
		h.log.Info("federation: hub accepted ephemeral connection", "peer", peerName)
	} else {
		h.log.Info("federation: hub accepted connection", "peer", peerName)
	}

	// Handle replay for reconnecting non-ephemeral peers. Ephemeral clients
	// never receive replayed messages regardless of whether LastID is set.
	if hs.LastID != "" && len(subChannels) > 0 && !hs.Ephemeral {
		go func() {
			for env := range h.ReplayFrom(hs.LastID, subChannels) {
				if sender != nil {
					sender.Enqueue(env)
				}
			}
		}()
	}

	// Wait for receiver to finish, then clean up.
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		<-receiver.Done()
		h.mu.Lock()
		delete(h.peers, peerName)
		h.mu.Unlock()
		if sender != nil {
			sender.Close()
		}
		_ = h.auditL.Log(audit.Event{Event: audit.EventPeerDisconnected, Peer: peerName})
	}()
}
