package federation

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/latencyhist"
	"github.com/wu/keyop-messenger/internal/storage"
	"github.com/wu/keyop-messenger/internal/tlsutil"
)

// Client dials a hub over gRPC and maintains two independent streams:
//
//   - Publish: outbound messages from this instance's local channel files.
//     For each configured publishChannel a channelReader watches the local
//     segment files (driven by the messenger's NotifyChannel calls), and a
//     pubCoordinator dispatches one batch at a time over the Publish stream,
//     advancing the per-channel "fedout-{hubAddr}.offset" file after each ack.
//
//   - Subscribe (optional): if subscribeChannels is non-empty, a PeerReceiver
//     consumes hub-pushed envelopes and hands them to localWriter.
//
// On disconnect the reconnect loop closes the active coordinator + readers
// and dials again with exponential backoff. Offsets persist across reconnects,
// so any message published locally during the disconnect is delivered on
// reconnect — there is no in-memory unacked window to lose.
type Client struct {
	instanceName      string
	tlsCfg            *tls.Config
	policy            *AtomicPolicy
	localWriter       func(*envelope.Envelope) error
	localBatchWriter  func([]*envelope.Envelope) error
	dedup             Deduplicator
	auditL            audit.AuditLogger
	log               logger
	maxBatchBytes     int
	dataDir           string
	subscribeChannels []string
	publishChannels   []string

	reconnectBase   time.Duration
	reconnectMax    time.Duration
	reconnectJitter float64

	hubAddr        string       // set once in ConnectWithReconnect before the reconnect goroutine starts
	reconnectCount atomic.Int64 // incremented on each successful reconnect (not the initial dial)

	// hubInstance holds the hub's authenticated identity (its TLS cert CN),
	// captured during the TLS handshake. Outbound readers use it to drop echoes
	// send-side. Empty until the first handshake completes, or on a non-TLS
	// connection; the hub's receive-side loop guard is the backstop for that
	// window. Accessed via hubInstanceName / setHubInstance.
	hubInstance atomic.Value // string

	// publish ack round-trip latency aggregate (client→hub transit): summed
	// send→ack durations and the sample count, measured on the client's own
	// clock so it is free of cross-host clock skew. Accumulated here on the
	// Client (not the pubCoordinator) so the totals survive reconnects. ackRTTWin
	// is the sliding-window histogram feeding the recent percentiles.
	ackRTTSumNs atomic.Int64
	ackRTTCount atomic.Int64
	ackRTTWin   *latencyhist.Window

	// publishSendFailures counts in-flight outbound batches that failed to be
	// acked because the stream broke (excluding deliberate shutdown). A rising
	// value signals delivery disruption to the hub. Accumulated on the Client so
	// it survives reconnects.
	publishSendFailures atomic.Int64

	mu          sync.Mutex
	coordinator *pubCoordinator
	receiver    *PeerReceiver

	// readersByChannel maps each configured publishChannel to the
	// channelReader currently feeding the coordinator. Reset on each dial.
	readersByChannel map[string]*channelReader

	// grpcConn is created lazily on the first dial and reused across reconnects.
	grpcConnMu sync.Mutex
	grpcConn   *grpc.ClientConn

	stop       chan struct{}
	stopCtx    context.Context
	stopCancel context.CancelFunc
	wg         sync.WaitGroup
}

// NewClient constructs a Client that is ready to dial. Call Dial or
// ConnectWithReconnect to establish a connection.
//
// dataDir is the root of the local store ({dataDir}/channels/{channel}/...).
// Each entry in publishChannels gets its own outbound reader and offset file.
func NewClient(
	instanceName string,
	tlsCfg *tls.Config,
	policy *AtomicPolicy,
	localWriter func(*envelope.Envelope) error,
	localBatchWriter func([]*envelope.Envelope) error,
	dedup Deduplicator,
	auditL audit.AuditLogger,
	log logger,
	maxBatchBytes int,
	reconnectBase, reconnectMax time.Duration,
	reconnectJitter float64,
	subscribeChannels []string,
	publishChannels []string,
	dataDir string,
) *Client {
	stopCtx, stopCancel := context.WithCancel(context.Background())
	return &Client{
		instanceName:      instanceName,
		tlsCfg:            tlsCfg,
		policy:            policy,
		localWriter:       localWriter,
		localBatchWriter:  localBatchWriter,
		dedup:             dedup,
		auditL:            auditL,
		log:               log,
		maxBatchBytes:     maxBatchBytes,
		dataDir:           dataDir,
		reconnectBase:     reconnectBase,
		reconnectMax:      reconnectMax,
		reconnectJitter:   reconnectJitter,
		subscribeChannels: subscribeChannels,
		publishChannels:   publishChannels,
		stop:              make(chan struct{}),
		stopCtx:           stopCtx,
		stopCancel:        stopCancel,
		ackRTTWin:         latencyhist.NewWindow(),
	}
}

// Dial connects to hubAddr once, opens the streams, and starts the per-channel
// readers + coordinator. Returns once the streams are open; outbound delivery
// continues in the background.
func (c *Client) Dial(hubAddr string) error {
	return c.dial(hubAddr)
}

func (c *Client) dial(hubAddr string) error {
	conn, err := c.getOrCreateGRPCConn(hubAddr)
	if err != nil {
		return fmt.Errorf("federation: client grpc connect %s: %w", hubAddr, err)
	}

	stub := federationv1.NewFederationServiceClient(conn)
	outMD := metadata.Pairs("x-federation-instance", c.instanceName)

	// Open the Publish stream, declaring the channels this client publishes so the
	// hub can record and surface them. This is advisory metadata; the hub still
	// enforces its own allowlist per message.
	pubMD := outMD
	if len(c.publishChannels) > 0 {
		pubMD = outMD.Copy()
		for _, ch := range c.publishChannels {
			pubMD.Append(mdPublishChannelsKey, ch)
		}
	}
	pubCtx, pubCancel := context.WithCancel(c.stopCtx)
	pubStream, err := stub.Publish(metadata.NewOutgoingContext(pubCtx, pubMD))
	if err != nil {
		pubCancel()
		return fmt.Errorf("federation: client open publish stream %s: %w", hubAddr, err)
	}

	// Build per-channel readers feeding a coordinator on the Publish stream.
	readers, readersByChannel, err := c.buildOutboundReaders(hubAddr)
	if err != nil {
		pubCancel()
		return fmt.Errorf("federation: client build outbound readers: %w", err)
	}
	coordinator := newPubCoordinator(pubStream, pubCancel, c.log, readers, c.recordAckRTT, c.recordPublishSendFailure, c.auditL, hubAddr)
	coordinator.start()

	// Open the Subscribe stream if this client subscribes to channels.
	var receiver *PeerReceiver
	if c.localWriter != nil && len(c.subscribeChannels) > 0 {
		subCtx, subCancel := context.WithCancel(c.stopCtx)
		subStream, sErr := stub.Subscribe(metadata.NewOutgoingContext(subCtx, outMD))
		if sErr != nil {
			subCancel()
			coordinator.close()
			return fmt.Errorf("federation: client open subscribe stream %s: %w", hubAddr, sErr)
		}

		// Send the SubscribeRequest as the first frame on the Subscribe stream.
		if sErr := subStream.Send(&federationv1.SubscribeFrame{
			Payload: &federationv1.SubscribeFrame_Request{
				Request: &federationv1.SubscribeRequest{
					Version:   wireVersion,
					Subscribe: c.subscribeChannels,
				},
			},
		}); sErr != nil {
			subCancel()
			coordinator.close()
			return fmt.Errorf("federation: client send subscribe request %s: %w", hubAddr, sErr)
		}

		receiver = NewPeerReceiver(subStream, subCancel, c.policy, c.dedup, c.localWriter,
			c.localBatchWriter, c.auditL, c.log, hubAddr, c.instanceName, c.maxBatchBytes)
	}

	c.mu.Lock()
	oldReceiver := c.receiver
	c.coordinator = coordinator
	c.receiver = receiver
	c.readersByChannel = readersByChannel
	c.mu.Unlock()

	// Close the old receiver outside the lock; on reconnect the underlying stream
	// is already dead so this returns promptly.
	if oldReceiver != nil {
		oldReceiver.Close()
	}

	_ = c.auditL.Log(audit.Event{Event: audit.EventPeerConnected, PeerAddr: hubAddr})
	return nil
}

// buildOutboundReaders creates one channelReader per configured publishChannel.
// Each reader uses an offset file named "fedout-{hubAddr}.offset" within
// {dataDir}/subscribers/{channel}/. Returns the slice (ordered) and a map for
// NotifyChannel lookup.
func (c *Client) buildOutboundReaders(hubAddr string) ([]*channelReader, map[string]*channelReader, error) {
	if len(c.publishChannels) == 0 || c.dataDir == "" {
		return nil, map[string]*channelReader{}, nil
	}
	peerName := sanitizeForFilename(hubAddr)
	readers := make([]*channelReader, 0, len(c.publishChannels))
	byChannel := make(map[string]*channelReader, len(c.publishChannels))
	for _, ch := range c.publishChannels {
		channelDir := filepath.Join(c.dataDir, "channels", ch)
		offsetDir := filepath.Join(c.dataDir, "subscribers", ch)
		placeholder := make(chan sendReq, 1)
		r, err := newChannelReader(peerName, ch, channelDir, offsetDir, "fedout-",
			c.maxBatchBytes, placeholder, c.hubInstanceName, c.log)
		if err != nil {
			return nil, nil, fmt.Errorf("channel %q: %w", ch, err)
		}
		readers = append(readers, r)
		byChannel[ch] = r
	}
	return readers, byChannel, nil
}

// sanitizeForFilename derives a filename-safe form of s (e.g. a hub network
// address like "host:7740") for use as an offset filename. It delegates to
// storage.SanitizeForFilename so the rule has a single definition shared with
// the subscriber and channelReader offset paths.
func sanitizeForFilename(s string) string {
	return storage.SanitizeForFilename(s)
}

// setHubInstance records the hub's authenticated identity, captured at the TLS
// handshake. Safe for concurrent use.
func (c *Client) setHubInstance(cn string) { c.hubInstance.Store(cn) }

// hubInstanceName returns the hub's authenticated identity, or "" if it is not
// yet known (handshake pending, or a non-TLS connection).
func (c *Client) hubInstanceName() string {
	if v, ok := c.hubInstance.Load().(string); ok {
		return v
	}
	return ""
}

// getOrCreateGRPCConn returns the existing gRPC connection or creates a new one
// targeting hubAddr. The connection is reused across reconnects.
func (c *Client) getOrCreateGRPCConn(hubAddr string) (*grpc.ClientConn, error) {
	c.grpcConnMu.Lock()
	defer c.grpcConnMu.Unlock()
	if c.grpcConn != nil {
		return c.grpcConn, nil
	}
	conn, err := newGRPCClientConn(hubAddr, c.tlsCfg, c.maxBatchBytes, c.setHubInstance)
	if err != nil {
		return nil, err
	}
	c.grpcConn = conn
	return conn, nil
}

// ConnectWithReconnect dials hubAddr and reconnects automatically on disconnect.
// It returns after the first successful connection.
func (c *Client) ConnectWithReconnect(hubAddr string) error {
	c.hubAddr = hubAddr
	if err := c.dial(hubAddr); err != nil {
		return err
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		backoff := c.reconnectBase
		attempt := 0
		for {
			c.mu.Lock()
			coord := c.coordinator
			c.mu.Unlock()

			select {
			case <-c.stop:
				return
			case <-coord.Done():
			}

			// Tear down the current connection's readers and coordinator.
			// Offset files persist; they will be reused by the next dial.
			coord.close()

			unacked := c.UnackedBytes()
			_ = c.auditL.Log(audit.Event{
				Event:    audit.EventPeerDisconnected,
				PeerAddr: hubAddr,
				Detail:   fmt.Sprintf("unacked_bytes=%d", unacked),
			})
			c.log.Warn("federation: client disconnected, reconnecting",
				"hub", hubAddr, "unacked_bytes", unacked)

			// #nosec G404 -- math/rand is appropriate for non-cryptographic jitter
			jitter := time.Duration(float64(backoff) * c.reconnectJitter * (rand.Float64()*2 - 1))
			sleep := backoff + jitter
			if sleep < 0 {
				sleep = 0
			}
			select {
			case <-c.stop:
				return
			case <-time.After(sleep):
			}

			for {
				select {
				case <-c.stop:
					return
				default:
				}
				attempt++
				dErr := c.dial(hubAddr)
				if dErr == nil {
					break
				}
				if c.stopCtx.Err() != nil {
					return
				}
				c.log.Error("federation: client reconnect failed", "err", dErr, "attempt", attempt)
				_ = c.auditL.Log(audit.Event{
					Event:    audit.EventPeerConnected,
					PeerAddr: hubAddr,
					Detail:   fmt.Sprintf("attempt=%d err=%s", attempt, dErr.Error()),
				})
				backoff = minDuration(backoff*2, c.reconnectMax)
				// #nosec G404 -- math/rand is appropriate for non-cryptographic jitter
				jitter = time.Duration(float64(backoff) * c.reconnectJitter * (rand.Float64()*2 - 1))
				sleep = backoff + jitter
				if sleep < 0 {
					sleep = c.reconnectBase
				}
				select {
				case <-c.stop:
					return
				case <-time.After(sleep):
				}
			}

			c.reconnectCount.Add(1)
			backoff = c.reconnectBase
			attempt = 0
		}
	}()
	return nil
}

// NotifyChannel wakes the channelReader for channel, if this client has one.
// Called by the Messenger after every local write so that outbound delivery
// resumes promptly without waiting for a filesystem-watcher tick.
func (c *Client) NotifyChannel(channel string) {
	c.mu.Lock()
	r, ok := c.readersByChannel[channel]
	c.mu.Unlock()
	if ok {
		r.notify()
	}
}

// AllowPublish reports whether the given channel is in this client's
// configured publish set. An empty publish list means "no channels" — see
// DESIGN §6.5 and config.ClientHubRef.Publish.
func (c *Client) AllowPublish(channel string) bool {
	for _, ch := range c.publishChannels {
		if ch == channel {
			return true
		}
	}
	return false
}

// HubAddr returns the address this client was configured to dial.
func (c *Client) HubAddr() string { return c.hubAddr }

// Connected reports whether the Publish stream is currently active.
func (c *Client) Connected() bool {
	c.mu.Lock()
	coord := c.coordinator
	c.mu.Unlock()
	if coord == nil {
		return false
	}
	select {
	case <-coord.Done():
		return false
	default:
		return true
	}
}

// ReconnectCount returns the number of successful reconnections after the
// initial dial. The first connection is not counted.
func (c *Client) ReconnectCount() int64 { return c.reconnectCount.Load() }

// recordAckRTT adds one publish ack round-trip sample (the send→ack elapsed
// time) to the client's running aggregate. Called by the pubCoordinator after
// each PublishBatch is acknowledged by the hub.
func (c *Client) recordAckRTT(d time.Duration) {
	c.ackRTTSumNs.Add(int64(d))
	c.ackRTTCount.Add(1)
	c.ackRTTWin.Observe(d)
}

// AckRTT returns the running publish ack round-trip aggregate for this hub
// connection: the summed send→ack duration in nanoseconds and the sample count.
// The mean is sumNanos/count (guard count == 0).
func (c *Client) AckRTT() (sumNanos, count int64) {
	return c.ackRTTSumNs.Load(), c.ackRTTCount.Load()
}

// PublishRTTBuckets returns the publish ack round-trip histogram buckets over
// the current trailing window, for computing recent percentiles.
func (c *Client) PublishRTTBuckets() []int64 {
	return c.ackRTTWin.LiveBuckets()
}

// recordPublishSendFailure increments the outbound delivery-failure count.
// Called by the pubCoordinator when an in-flight batch is lost to a broken stream.
func (c *Client) recordPublishSendFailure() {
	c.publishSendFailures.Add(1)
}

// PublishSendFailures returns the number of outbound batches that failed to be
// acked because the stream broke, since this Client started.
func (c *Client) PublishSendFailures() int64 {
	return c.publishSendFailures.Load()
}

// UnackedBytes returns the total bytes published locally on this client's
// outbound channels that have not yet been acknowledged by the hub. It is
// computed as sum(streamEnd - persistedOffset) across all configured
// publishChannels. Returns 0 when no channels are configured or no data dir
// is set. The value is read from disk and is a slightly-stale snapshot.
func (c *Client) UnackedBytes() int64 {
	if c.dataDir == "" || len(c.publishChannels) == 0 {
		return 0
	}
	peerName := sanitizeForFilename(c.hubAddr)
	var total int64
	for _, ch := range c.publishChannels {
		channelDir := filepath.Join(c.dataDir, "channels", ch)
		offsetPath := filepath.Join(c.dataDir, "subscribers", ch, "fedout-"+peerName+".offset")
		end, err := storage.ChannelStreamEnd(channelDir)
		if err != nil {
			continue
		}
		var off int64
		if storage.OffsetFileExists(offsetPath) {
			if v, err := storage.ReadOffset(offsetPath); err == nil {
				off = v
			}
		}
		if lag := end - off; lag > 0 {
			total += lag
		}
	}
	return total
}

// Close stops the reconnect loop and the current connection.
func (c *Client) Close() {
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	c.stopCancel() // cancels all open gRPC streams
	c.mu.Lock()
	coord := c.coordinator
	receiver := c.receiver
	c.mu.Unlock()
	if coord != nil {
		coord.close()
	}
	if receiver != nil {
		receiver.Close()
	}
	c.wg.Wait()
	c.grpcConnMu.Lock()
	if c.grpcConn != nil {
		_ = c.grpcConn.Close()
	}
	c.grpcConnMu.Unlock()
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// cnCapturingCreds wraps a TransportCredentials and reports the server leaf
// certificate's CN after each successful client handshake via onCN. This lets
// the client learn the hub's authenticated identity from the same source of
// truth the hub uses for peers — the TLS certificate — for send-side loop
// filtering. Clone is overridden so gRPC's internal credential cloning does not
// strip the wrapper.
type cnCapturingCreds struct {
	credentials.TransportCredentials
	onCN func(string)
}

func (c cnCapturingCreds) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, authInfo, err := c.TransportCredentials.ClientHandshake(ctx, authority, rawConn)
	if err == nil && c.onCN != nil {
		if tlsInfo, ok := authInfo.(credentials.TLSInfo); ok && len(tlsInfo.State.PeerCertificates) > 0 {
			if cn := tlsutil.ExtractCN(tlsInfo.State.PeerCertificates[0]); cn != "" {
				c.onCN(cn)
			}
		}
	}
	return conn, authInfo, err
}

func (c cnCapturingCreds) Clone() credentials.TransportCredentials {
	return cnCapturingCreds{TransportCredentials: c.TransportCredentials.Clone(), onCN: c.onCN}
}

// newGRPCClientConn creates a gRPC client connection to target. Uses TLS when
// tlsCfg is non-nil, otherwise uses insecure (plaintext) credentials.
// The connection is created with lazy dialing; the actual TCP connection is
// established on the first RPC call. onCN, when non-nil, is invoked with the
// hub's certificate CN after each successful TLS handshake.
func newGRPCClientConn(target string, tlsCfg *tls.Config, maxBatchBytes int, onCN func(string)) (*grpc.ClientConn, error) {
	var creds credentials.TransportCredentials
	if tlsCfg != nil {
		creds = cnCapturingCreds{TransportCredentials: credentials.NewTLS(tlsCfg), onCN: onCN}
	} else {
		creds = insecure.NewCredentials()
	}
	// Match the hub's raised frame limit so receiving a batch up to
	// maxRecordBytes does not fail with ResourceExhausted (gRPC's client
	// receive default is also 4 MiB).
	limit := grpcMessageLimit(maxBatchBytes)
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(limit),
			grpc.MaxCallSendMsgSize(limit),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
}
