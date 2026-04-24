package federation

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/tlsutil"
)

// connDetail builds a Detail string for audit events from connection metadata.
func connDetail(peerAddr string, subChannels, pubChannels []string) string {
	s := "addr=" + peerAddr
	if len(subChannels) > 0 {
		s += " sub=[" + strings.Join(subChannels, ",") + "]"
	}
	if len(pubChannels) > 0 {
		s += " pub=[" + strings.Join(pubChannels, ",") + "]"
	}
	return s
}

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
	name        string
	receiver    *PeerReceiver
	coordinator *clientCoordinator // non-nil when peer has subscriptions
	policy      *AtomicPolicy
	conn        *websocket.Conn
	connWriteMu *sync.Mutex
}

// Hub accepts incoming WebSocket connections and enforces the allowlist and
// receive policy. Hub-to-peer delivery uses a file-reader pull model: one
// channelReader goroutine per (peer, channel) reads segment files and delivers
// batches via a clientCoordinator, writing per-peer byte offsets to
//
//	{dataDir}/subscribers/{channel}/fed-{peerName}.offset
//
// The compactor automatically includes federation peers in compaction boundary
// calculations because it reads all files in subscribers/{channel}/.
type Hub struct {
	instanceName       string
	tlsCfg             *tls.Config
	localWriter        func(*envelope.Envelope) error
	dedup              Deduplicator
	auditL             audit.AuditLogger
	log                logger
	sendBufSize        int
	maxBatchBytes      int
	dataDir            string
	fedClientOffsetTTL time.Duration

	mu       sync.RWMutex
	cfg      HubConfig
	peers    map[string]*peerEntry
	listener net.Listener
	httpSrv  *http.Server

	// notifyMu protects notifyRegistry; use RLock for reads (NotifyChannel),
	// Lock for writes (serveConn register/deregister).
	notifyMu       sync.RWMutex
	notifyRegistry map[string][]*channelReader // channel → readers for that channel

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
		instanceName:   instanceName,
		cfg:            cfg,
		tlsCfg:         tlsCfg,
		localWriter:    localWriter,
		dedup:          dedup,
		auditL:         auditL,
		log:            log,
		sendBufSize:    sendBufSize,
		maxBatchBytes:  maxBatchBytes,
		dataDir:        dataDir,
		peers:          make(map[string]*peerEntry),
		notifyRegistry: make(map[string][]*channelReader),
		stop:           make(chan struct{}),
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

	if h.fedClientOffsetTTL > 0 && h.dataDir != "" {
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			h.runTTLSweep(h.fedClientOffsetTTL)
		}()
	}

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		_ = srv.Serve(ln)
	}()
	return nil
}

// SetFedClientOffsetTTL configures the TTL for disconnected federation client
// offset files. Must be called before Listen. A zero or negative value disables
// the sweep.
func (h *Hub) SetFedClientOffsetTTL(ttl time.Duration) {
	h.fedClientOffsetTTL = ttl
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

// NotifyChannel wakes all channelReader goroutines registered for channel.
// Called by messenger.go after every write to that channel.
func (h *Hub) NotifyChannel(channel string) {
	h.notifyMu.RLock()
	defer h.notifyMu.RUnlock()
	for _, r := range h.notifyRegistry[channel] {
		r.notify()
	}
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
// starts the receiver and (if the peer has subscriptions) a clientCoordinator.
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

	if !cfg.IsPeerAllowed(peerName) {
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

	// Compute effective channels: intersection of peer's Subscribe request and
	// the hub's per-peer allowlist.
	subChannels := effectiveSubscribeChannels(hs.Subscribe, peerName, cfg)
	pubChannels := publishChannelsFor(peerName, cfg)
	policy := NewAtomicPolicy(ForwardPolicy{
		Forward: subChannels,
		Receive: pubChannels,
	})

	connWriteMu := &sync.Mutex{}

	// Set up inbound receiver. When the peer also subscribes, the receiver owns
	// all reads and routes text-frame acks to the coordinator via ackCh.
	var receiver *PeerReceiver
	var coordinator *clientCoordinator

	if len(subChannels) > 0 && h.dataDir != "" {
		ackCh := make(chan AckMsg, 4)
		receiver = newPeerReceiverWithAck(conn, connWriteMu, policy, h.dedup, h.localWriter,
			h.auditL, h.log, peerName, h.maxBatchBytes, ackCh)

		readers, err := h.buildChannelReaders(peerName, subChannels, ackCh, connWriteMu, conn)
		if err != nil {
			h.log.Error("federation: hub build channel readers", "peer", peerName, "err", err)
			_ = conn.Close()
			return
		}
		coordinator = readers
		coordinator.start()

		// Register channel readers into the notify registry.
		h.notifyMu.Lock()
		for _, r := range coordinator.readers {
			h.notifyRegistry[r.channel] = append(h.notifyRegistry[r.channel], r)
		}
		h.notifyMu.Unlock()
	} else {
		// No subscriptions: only receive inbound messages from the peer.
		receiver = NewPeerReceiver(conn, connWriteMu, policy, h.dedup, h.localWriter,
			h.auditL, h.log, peerName, h.maxBatchBytes)
	}

	entry := &peerEntry{
		name:        peerName,
		receiver:    receiver,
		coordinator: coordinator,
		policy:      policy,
		conn:        conn,
		connWriteMu: connWriteMu,
	}

	peerAddr := conn.RemoteAddr().String()
	connectedAt := time.Now()

	h.mu.Lock()
	h.peers[peerName] = entry
	h.mu.Unlock()

	_ = h.auditL.Log(audit.Event{
		Event:    audit.EventClientConnected,
		Peer:     peerName,
		PeerAddr: peerAddr,
		Detail:   connDetail(peerAddr, subChannels, pubChannels),
	})
	if hs.Ephemeral {
		h.log.Info("federation: hub accepted ephemeral connection", "peer", peerName, "addr", peerAddr)
	} else {
		h.log.Info("federation: hub accepted connection", "peer", peerName, "addr", peerAddr)
	}

	// Wait for receiver to finish, then clean up.
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		<-receiver.Done()

		// Deregister channel readers from the notify registry.
		if coordinator != nil {
			h.notifyMu.Lock()
			for _, r := range coordinator.readers {
				readers := h.notifyRegistry[r.channel]
				for i, rr := range readers {
					if rr == r {
						h.notifyRegistry[r.channel] = append(readers[:i], readers[i+1:]...)
						break
					}
				}
				if len(h.notifyRegistry[r.channel]) == 0 {
					delete(h.notifyRegistry, r.channel)
				}
			}
			h.notifyMu.Unlock()
			coordinator.close()
		}

		h.mu.Lock()
		delete(h.peers, peerName)
		h.mu.Unlock()

		duration := time.Since(connectedAt).Round(time.Millisecond)
		detail := "duration=" + duration.String()
		if err := receiver.Err(); err != nil {
			detail += " err=" + err.Error()
		}
		_ = h.auditL.Log(audit.Event{
			Event:    audit.EventPeerDisconnected,
			Peer:     peerName,
			PeerAddr: peerAddr,
			Detail:   detail,
		})
	}()
}

// buildChannelReaders creates a clientCoordinator with one channelReader per
// subscribed channel. The coordinator's requestCh is wired into all readers.
func (h *Hub) buildChannelReaders(
	peerName string,
	subChannels []string,
	ackCh <-chan AckMsg,
	connWriteMu *sync.Mutex,
	conn *websocket.Conn,
) (*clientCoordinator, error) {
	var readers []*channelReader
	for _, ch := range subChannels {
		channelDir := filepath.Join(h.dataDir, "channels", ch)
		offsetDir := filepath.Join(h.dataDir, "subscribers", ch)

		// Placeholder requestCh; newClientCoordinator will replace it.
		placeholder := make(chan sendReq, 1)
		r, err := newChannelReader(peerName, ch, channelDir, offsetDir,
			h.maxBatchBytes, placeholder, h.log)
		if err != nil {
			return nil, fmt.Errorf("channel %q: %w", ch, err)
		}
		readers = append(readers, r)
	}

	cc := newClientCoordinator(conn, connWriteMu, ackCh, h.maxBatchBytes, h.log, readers)
	return cc, nil
}

// runTTLSweep periodically deletes fed-*.offset files whose mtime is older
// than ttl. This prevents stale files from indefinitely blocking compaction
// for peers that disconnect and never reconnect.
//
// The sweep runs hourly (or less if ttl < 1h). It logs each deletion at Info.
// Exits when h.stop is closed.
func (h *Hub) runTTLSweep(ttl time.Duration) {
	interval := time.Hour
	if ttl < interval {
		interval = ttl / 2
		if interval < time.Minute {
			interval = time.Minute
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-h.stop:
			return
		case <-ticker.C:
			h.sweepStaleOffsets(ttl)
		}
	}
}

// sweepStaleOffsets scans all subscribers/{channel}/fed-*.offset files and
// deletes those whose mtime is older than ttl.
func (h *Hub) sweepStaleOffsets(ttl time.Duration) {
	if h.dataDir == "" {
		return
	}
	subsDir := filepath.Join(h.dataDir, "subscribers")
	entries, err := os.ReadDir(subsDir)
	if err != nil {
		if !os.IsNotExist(err) {
			h.log.Error("federation: TTL sweep read subscribers dir", "err", err)
		}
		return
	}

	cutoff := time.Now().Add(-ttl)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		channelDir := filepath.Join(subsDir, e.Name())
		files, err := os.ReadDir(channelDir)
		if err != nil {
			continue
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasPrefix(f.Name(), "fed-") || !strings.HasSuffix(f.Name(), ".offset") {
				continue
			}
			info, err := f.Info()
			if err != nil {
				continue
			}
			if info.ModTime().Before(cutoff) {
				path := filepath.Join(channelDir, f.Name())
				peerName := strings.TrimSuffix(strings.TrimPrefix(f.Name(), "fed-"), ".offset")
				age := time.Since(info.ModTime()).Round(time.Minute)
				if rmErr := os.Remove(path); rmErr == nil {
					h.log.Info("federation: TTL sweep removed stale offset",
						"peer", peerName, "channel", e.Name(), "age", age)
				} else {
					h.log.Error("federation: TTL sweep remove failed",
						"path", path, "err", rmErr)
				}
			}
		}
	}
}
