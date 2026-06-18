package federation

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
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

// Hub accepts incoming gRPC connections and enforces the allowlist and receive
// policy. Hub-to-peer delivery uses a file-reader pull model: one channelReader
// goroutine per (peer, channel) reads segment files and delivers batches via a
// clientCoordinator, writing per-peer byte offsets to
//
//	{dataDir}/subscribers/{channel}/fed-{peerName}.offset
//
// Hub implements FederationServiceServer: the Publish RPC handles inbound
// publishes from clients, and the Subscribe RPC handles outbound delivery to
// subscribing clients.
type Hub struct {
	federationv1.UnimplementedFederationServiceServer

	tlsCfg             *tls.Config
	localWriter        func(*envelope.Envelope) error
	dedup              Deduplicator
	auditL             audit.AuditLogger
	log                logger
	sendBufSize        int
	maxBatchBytes      int
	dataDir            string
	fedClientOffsetTTL time.Duration

	mu      sync.RWMutex
	cfg     HubConfig
	grpcSrv *grpc.Server
	lis     net.Listener

	// notifyMu protects notifyRegistry; use RLock for reads (NotifyChannel),
	// Lock for writes (Subscribe handler register/deregister).
	notifyMu       sync.RWMutex
	notifyRegistry map[string][]*channelReader // channel → readers for that channel

	stop      chan struct{}
	wg        sync.WaitGroup // Serve goroutine + TTL sweep
	handlerWg sync.WaitGroup // active Publish/Subscribe RPC handler goroutines

	// handlerGateMu protects handlerGateClosed. Set closed=true under the lock
	// before calling handlerWg.Wait() so that any late-starting handler goroutine
	// (created by gRPC but not yet scheduled when Stop() returns) sees the flag
	// and skips Add(1), preventing the Add/Wait data race on the WaitGroup.
	handlerGateMu     sync.Mutex
	handlerGateClosed bool
}

// NewHub constructs a Hub. Call Listen to start accepting connections.
func NewHub(
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
		cfg:            cfg,
		tlsCfg:         tlsCfg,
		localWriter:    localWriter,
		dedup:          dedup,
		auditL:         auditL,
		log:            log,
		sendBufSize:    sendBufSize,
		maxBatchBytes:  maxBatchBytes,
		dataDir:        dataDir,
		notifyRegistry: make(map[string][]*channelReader),
		stop:           make(chan struct{}),
	}
}

// SetFedClientOffsetTTL configures the TTL for disconnected federation client
// offset files. Must be called before Listen.
func (h *Hub) SetFedClientOffsetTTL(ttl time.Duration) {
	h.fedClientOffsetTTL = ttl
}

// newGRPCServer creates a configured gRPC server and registers the hub on it.
func (h *Hub) newGRPCServer() *grpc.Server {
	var opts []grpc.ServerOption
	if h.tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(h.tlsCfg)))
	}
	opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle: 5 * time.Minute,
		Time:              30 * time.Second,
		Timeout:           10 * time.Second,
	}))
	// Raise the receive limit above gRPC's 4 MiB default so peers can publish
	// records up to maxRecordBytes; the default would reject a larger batch with
	// ResourceExhausted, dropping the stream and triggering an endless reconnect.
	limit := grpcMessageLimit(h.maxBatchBytes)
	opts = append(opts, grpc.MaxRecvMsgSize(limit), grpc.MaxSendMsgSize(limit))
	srv := grpc.NewServer(opts...)
	federationv1.RegisterFederationServiceServer(srv, h)
	return srv
}

// Listen starts a gRPC listener on addr and returns immediately.
func (h *Hub) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("federation: hub listen %s: %w", addr, err)
	}
	h.serveOn(ln)
	return nil
}

// ServeOn registers the gRPC server on lis and starts background goroutines.
// Use in tests with a custom listener (e.g. bufconn).
func (h *Hub) ServeOn(lis net.Listener) {
	h.serveOn(lis)
}

func (h *Hub) serveOn(lis net.Listener) {
	grpcSrv := h.newGRPCServer()

	h.mu.Lock()
	h.lis = lis
	h.grpcSrv = grpcSrv
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
		_ = grpcSrv.Serve(lis)
	}()
}

// Addr returns the listener address, or empty string if not listening.
func (h *Hub) Addr() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.lis == nil {
		return ""
	}
	return h.lis.Addr().String()
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

// handlerEnter increments the handler WaitGroup and returns true if the hub
// is still open. Returns false when the hub is closing; callers must return
// immediately without calling handlerExit.
func (h *Hub) handlerEnter() bool {
	h.handlerGateMu.Lock()
	defer h.handlerGateMu.Unlock()
	if h.handlerGateClosed {
		return false
	}
	h.handlerWg.Add(1)
	return true
}

// handlerExit must be called (via defer) by any goroutine that called
// handlerEnter and received true.
func (h *Hub) handlerExit() { h.handlerWg.Done() }

// Close stops the listener and all active peer connections, then waits for
// all in-flight handler goroutines to finish.
func (h *Hub) Close() error {
	close(h.stop)
	h.mu.Lock()
	if h.grpcSrv != nil {
		h.grpcSrv.Stop()
	}
	h.mu.Unlock()
	h.wg.Wait() // Serve goroutine + TTL sweep

	// Close the handler gate before waiting. Any handler goroutine that gRPC
	// spawned but that hasn't run yet will see the closed flag and skip Add(1),
	// preventing the Add/Wait race on the WaitGroup.
	h.handlerGateMu.Lock()
	h.handlerGateClosed = true
	h.handlerGateMu.Unlock()

	h.handlerWg.Wait() // Publish + Subscribe handler goroutines
	return nil
}

// authenticatePeer returns the peer's identity for a Publish or Subscribe
// stream. On a TLS connection the certificate CN is the only acceptable
// identity (tamper-proof); a TLS connection with no peer certificate or an
// empty CN is rejected. The x-federation-instance metadata header is only
// honored on non-TLS connections.
func (h *Hub) authenticatePeer(ctx context.Context) (string, error) {
	if p, ok := peer.FromContext(ctx); ok {
		if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			if len(tlsInfo.State.PeerCertificates) == 0 {
				return "", errors.New("TLS connection has no peer certificate")
			}
			cn := tlsutil.ExtractCN(tlsInfo.State.PeerCertificates[0])
			if cn == "" {
				return "", errors.New("TLS peer certificate has empty CN")
			}
			return cn, nil
		}
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("missing gRPC metadata")
	}
	vals := md.Get("x-federation-instance")
	if len(vals) == 0 {
		return "", errors.New("missing x-federation-instance metadata")
	}
	return vals[0], nil
}

// peerAddrFromContext extracts the remote address string from gRPC peer info.
func peerAddrFromContext(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return "unknown"
}

// Publish implements FederationServiceServer. It receives envelope batches from
// a client, deduplicates, enforces the publish policy, writes to local storage,
// and sends a PublishAck after each batch.
func (h *Hub) Publish(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
	if !h.handlerEnter() {
		return status.Error(codes.Unavailable, "hub is shutting down")
	}
	defer h.handlerExit()

	peerName, err := h.authenticatePeer(stream.Context())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "federation: %v", err)
	}

	h.mu.RLock()
	cfg := h.cfg
	h.mu.RUnlock()

	if !cfg.IsPeerAllowed(peerName) {
		_ = h.auditL.Log(audit.Event{Event: audit.EventClientRejected, Peer: peerName})
		h.log.Warn("federation: hub rejected publish connection", "peer", peerName)
		return status.Errorf(codes.PermissionDenied, "not in allowlist")
	}

	pubChannels := publishChannelsFor(peerName, cfg)
	policy := NewAtomicPolicy(ForwardPolicy{Receive: pubChannels})
	peerAddr := peerAddrFromContext(stream.Context())

	_ = h.auditL.Log(audit.Event{
		Event:    audit.EventClientConnected,
		Peer:     peerName,
		PeerAddr: peerAddr,
		Detail:   connDetail(peerAddr, nil, pubChannels),
	})
	h.log.Info("federation: hub accepted publish connection", "peer", peerName, "addr", peerAddr)

	connectedAt := time.Now()
	var lastErr error

	for {
		batch, recvErr := stream.Recv()
		if recvErr != nil {
			if !errors.Is(recvErr, io.EOF) {
				lastErr = recvErr
			}
			break
		}

		var lastID string
		for _, rec := range batch.Records {
			env, uErr := envelope.Unmarshal(rec)
			if uErr != nil {
				h.log.Error("federation: hub unmarshal", "peer", peerName, "err", uErr)
				continue
			}

			if h.dedup.SeenOrAdd(env.ID) {
				continue
			}

			if !policy.AllowReceive(env.Channel) {
				h.log.Warn("federation: receive policy violation",
					"channel", env.Channel, "peer", peerName, "id", env.ID)
				_ = h.auditL.Log(audit.Event{
					Event:     audit.EventPolicyViolation,
					Channel:   env.Channel,
					Peer:      peerName,
					Direction: "inbound",
					MessageID: env.ID,
				})
				lastID = env.ID
				continue
			}

			envCopy := env
			if wErr := h.localWriter(&envCopy); wErr != nil {
				h.log.Error("federation: hub local write", "id", env.ID, "err", wErr, "channel", env.Channel)
				lastID = env.ID
				continue
			}

			_ = h.auditL.Log(audit.Event{
				Event:     audit.EventForward,
				MessageID: env.ID,
				Channel:   env.Channel,
				Peer:      peerName,
				Direction: "inbound",
			})
			lastID = env.ID
		}

		if sendErr := stream.Send(&federationv1.PublishAck{LastId: lastID}); sendErr != nil {
			lastErr = sendErr
			break
		}
	}

	duration := time.Since(connectedAt).Round(time.Millisecond)
	detail := "duration=" + duration.String()
	if lastErr != nil {
		detail += " err=" + lastErr.Error()
	}
	_ = h.auditL.Log(audit.Event{
		Event:    audit.EventPeerDisconnected,
		Peer:     peerName,
		PeerAddr: peerAddr,
		Detail:   detail,
	})

	return nil
}

// Subscribe implements FederationServiceServer. It reads the initial
// SubscribeRequest, validates the peer, starts channelReaders for each
// subscribed channel, and streams EnvelopeBatch messages to the client.
// Acks from the client are routed to the clientCoordinator via an internal channel.
func (h *Hub) Subscribe(stream grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]) error {
	if !h.handlerEnter() {
		return status.Error(codes.Unavailable, "hub is shutting down")
	}
	defer h.handlerExit()

	peerName, err := h.authenticatePeer(stream.Context())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "federation: %v", err)
	}

	// First frame must be a SubscribeRequest.
	frame, err := stream.Recv()
	if err != nil {
		return err
	}
	req := frame.GetRequest()
	if req == nil {
		return status.Error(codes.InvalidArgument, "first frame must be SubscribeRequest")
	}

	h.mu.RLock()
	cfg := h.cfg
	h.mu.RUnlock()

	if !cfg.IsPeerAllowed(peerName) {
		_ = h.auditL.Log(audit.Event{Event: audit.EventClientRejected, Peer: peerName})
		h.log.Warn("federation: hub rejected subscribe connection", "peer", peerName)
		return status.Errorf(codes.PermissionDenied, "not in allowlist")
	}

	subChannels := effectiveSubscribeChannels(req.Subscribe, peerName, cfg)
	if len(subChannels) == 0 || h.dataDir == "" {
		// No effective subscriptions: nothing to deliver, return immediately.
		return nil
	}

	peerAddr := peerAddrFromContext(stream.Context())

	// ackCh is written by the ack-reader goroutine (below) and read by the coordinator.
	ackCh := make(chan struct{}, 4)

	readers, err := h.buildChannelReaders(peerName, subChannels, ackCh, stream)
	if err != nil {
		h.log.Error("federation: hub build channel readers", "peer", peerName, "err", err)
		return status.Errorf(codes.Internal, "build channel readers: %v", err)
	}

	coordinator := readers
	coordinator.start()

	// Register channelReaders into the notify registry.
	h.notifyMu.Lock()
	for _, r := range coordinator.readers {
		h.notifyRegistry[r.channel] = append(h.notifyRegistry[r.channel], r)
	}
	h.notifyMu.Unlock()

	_ = h.auditL.Log(audit.Event{
		Event:    audit.EventClientConnected,
		Peer:     peerName,
		PeerAddr: peerAddr,
		Detail:   connDetail(peerAddr, subChannels, nil),
	})
	if req.Ephemeral {
		h.log.Info("federation: hub accepted ephemeral subscribe", "peer", peerName, "addr", peerAddr)
	} else {
		h.log.Info("federation: hub accepted subscribe connection", "peer", peerName, "addr", peerAddr)
	}

	connectedAt := time.Now()

	// Ack-reader goroutine: owns all reads on the Subscribe server stream.
	// Forwards acks to the coordinator; closes ackCh when the stream ends.
	ackRecvDone := make(chan struct{})
	go func() {
		defer close(ackRecvDone)
		defer close(ackCh)
		for {
			f, recvErr := stream.Recv()
			if recvErr != nil {
				return // stream ended (EOF, cancelled, or error)
			}
			if f.GetAck() != nil {
				select {
				case ackCh <- struct{}{}:
				default:
				}
			}
		}
	}()

	// Block until the coordinator exits (connection lost or hub is stopping).
	<-coordinator.done

	// Deregister channelReaders from the notify registry.
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

	duration := time.Since(connectedAt).Round(time.Millisecond)
	_ = h.auditL.Log(audit.Event{
		Event:    audit.EventPeerDisconnected,
		Peer:     peerName,
		PeerAddr: peerAddr,
		Detail:   "duration=" + duration.String(),
	})

	// Wait for the ack-reader goroutine to exit before returning so that gRPC
	// can cleanly terminate the stream without dangling goroutines.
	<-ackRecvDone

	return nil
}

// buildChannelReaders creates a clientCoordinator with one channelReader per
// subscribed channel.
func (h *Hub) buildChannelReaders(
	peerName string,
	subChannels []string,
	ackCh <-chan struct{},
	stream grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch],
) (*clientCoordinator, error) {
	var readers []*channelReader
	for _, ch := range subChannels {
		channelDir := filepath.Join(h.dataDir, "channels", ch)
		offsetDir := filepath.Join(h.dataDir, "subscribers", ch)

		placeholder := make(chan sendReq, 1)
		r, err := newChannelReader(peerName, ch, channelDir, offsetDir, "fed-",
			h.maxBatchBytes, placeholder, h.log)
		if err != nil {
			return nil, fmt.Errorf("channel %q: %w", ch, err)
		}
		readers = append(readers, r)
	}

	cc := newClientCoordinator(stream, ackCh, h.maxBatchBytes, h.log, readers)
	return cc, nil
}

// runTTLSweep periodically deletes fed-*.offset files whose mtime is older
// than ttl. Exits when h.stop is closed.
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
