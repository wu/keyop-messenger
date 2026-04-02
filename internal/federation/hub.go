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

	"github.com/gorilla/websocket"
	"github.com/keyop/keyop-messenger/internal/audit"
	"github.com/keyop/keyop-messenger/internal/envelope"
	"github.com/keyop/keyop-messenger/internal/tlsutil"
)

const wireVersion = "1"

// peerEntry tracks an active peer connection on the hub.
type peerEntry struct {
	name     string
	sender   *PeerSender   // may be nil if this peer only sends to us
	receiver *PeerReceiver // may be nil if we only send to this peer
	policy   *AtomicPolicy
	conn     *websocket.Conn
}

// Hub accepts incoming WebSocket connections, enforces the allowlist and receive
// policy, and implements HubApplier for PolicyWatcher hot-reload.
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

	mu       sync.RWMutex
	cfg      HubConfig
	peers    map[string]*peerEntry // keyed by instance name
	listener net.Listener
	httpSrv  *http.Server

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
		instanceName:  instanceName,
		cfg:           cfg,
		tlsCfg:        tlsCfg,
		localWriter:   localWriter,
		dedup:         dedup,
		auditL:        auditL,
		log:           log,
		sendBufSize:   sendBufSize,
		maxBatchBytes: maxBatchBytes,
		dataDir:       dataDir,
		peers:         make(map[string]*peerEntry),
		stop:          make(chan struct{}),
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

// ApplyPolicy atomically updates the hub's policy configuration (implements
// HubApplier). Existing peer AtomicPolicies are updated in place; newly added
// peer hubs are dialed; removed peers are drained then closed.
func (h *Hub) ApplyPolicy(newCfg HubConfig) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Build set of new peer addrs.
	newPeerSet := make(map[string]PeerHubConfig, len(newCfg.PeerHubs))
	for _, ph := range newCfg.PeerHubs {
		newPeerSet[ph.Addr] = ph
	}

	// Update or remove existing peers.
	for name, entry := range h.peers {
		ph, ok := newPeerSet[name]
		if !ok {
			// Peer removed: drain-then-close.
			go h.drainAndClose(entry)
			delete(h.peers, name)
			continue
		}
		// Update channel lists atomically.
		if entry.policy != nil {
			entry.policy.Store(ForwardPolicy{
				Forward: ph.Forward,
				Receive: ph.Receive,
			})
		}
	}

	// Remove allowlist-evicted clients.
	for name, entry := range h.peers {
		_, isPeer := newPeerSet[name]
		if !isPeer && !newCfg.IsClientAllowed(name) {
			go h.drainAndClose(entry)
			delete(h.peers, name)
		}
	}

	h.cfg = newCfg
}

func (h *Hub) drainAndClose(entry *peerEntry) {
	_ = entry.conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "policy updated"))
	_ = entry.conn.Close()
	_ = h.auditL.Log(audit.Event{Event: audit.EventClientDrain, Peer: entry.name})
}

// EnqueueToAll enqueues env to every peer sender whose forward policy permits
// the channel. Used by the Messenger layer (Phase 13) when publishing.
func (h *Hub) EnqueueToAll(env *envelope.Envelope) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, entry := range h.peers {
		if entry.sender != nil && entry.policy != nil && entry.policy.AllowForward(env.Channel) {
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
						f.Close()
						break outer
					}
				}
				f.Close()
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
					f.Close()
					return
				}
			}
			f.Close()
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
		conn.Close()
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

	allowed := cfg.IsClientAllowed(peerName)
	if !allowed {
		for _, ph := range cfg.PeerHubs {
			if ph.Addr == peerName || hostnameOf(ph.Addr) == peerName {
				allowed = true
				break
			}
		}
	}

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
		conn.Close()
		return
	}

	// Determine forward/receive policy for this peer (if it's a configured peer hub).
	policy := NewAtomicPolicy(ForwardPolicy{})
	for _, ph := range cfg.PeerHubs {
		if hostnameOf(ph.Addr) == peerName || ph.Addr == peerName {
			policy.Store(ForwardPolicy{Forward: ph.Forward, Receive: ph.Receive})
			break
		}
	}

	// Start receiver. Also start sender if the peer hub has a forward list.
	// When both share a conn, route acks via an internal channel so the
	// PeerReceiver owns all reads and avoids a concurrent-read race.
	fp := policy.p.Load()
	needSender := fp != nil && len(fp.Forward) > 0

	var receiver *PeerReceiver
	var sender *PeerSender
	if needSender {
		ackCh := make(chan AckMsg, 4)
		receiver = newPeerReceiverWithAck(conn, policy, h.dedup, h.localWriter,
			h.auditL, h.log, peerName, h.maxBatchBytes, ackCh)
		sender = newPeerSenderWithAck(conn, h.sendBufSize, h.maxBatchBytes, h.log, ackCh)
	} else {
		receiver = NewPeerReceiver(conn, policy, h.dedup, h.localWriter,
			h.auditL, h.log, peerName, h.maxBatchBytes)
	}

	entry := &peerEntry{
		name:     peerName,
		sender:   sender,
		receiver: receiver,
		policy:   policy,
		conn:     conn,
	}

	h.mu.Lock()
	h.peers[peerName] = entry
	h.mu.Unlock()

	_ = h.auditL.Log(audit.Event{Event: audit.EventClientConnected, Peer: peerName})
	h.log.Info("federation: hub accepted connection", "peer", peerName)

	// Handle replay for reconnecting peers.
	if hs.LastID != "" {
		fp := policy.p.Load()
		var fwdChannels []string
		if fp != nil {
			fwdChannels = fp.Forward
		}
		go func() {
			for env := range h.ReplayFrom(hs.LastID, fwdChannels) {
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

// hostnameOf extracts the hostname from "host:port" or returns s unchanged.
func hostnameOf(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}
