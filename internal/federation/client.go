package federation

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/envelope"
)

// Client dials a hub over gRPC, runs a PeerSender for outbound messages on the
// Publish stream, and reconnects with exponential backoff on disconnect. A
// PeerReceiver can be optionally started for hubs that also push messages back
// to the client, using the Subscribe stream.
type Client struct {
	instanceName      string
	tlsCfg            *tls.Config
	policy            *AtomicPolicy
	localWriter       func(*envelope.Envelope) error
	dedup             Deduplicator
	auditL            audit.AuditLogger
	log               logger
	sendBufSize       int
	maxBatchBytes     int
	subscribeChannels []string
	publishChannels   []string

	reconnectBase   time.Duration
	reconnectMax    time.Duration
	reconnectJitter float64

	mu       sync.Mutex
	sender   *PeerSender
	receiver *PeerReceiver // nil when no subscribe channels are configured

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
func NewClient(
	instanceName string,
	tlsCfg *tls.Config,
	policy *AtomicPolicy,
	localWriter func(*envelope.Envelope) error,
	dedup Deduplicator,
	auditL audit.AuditLogger,
	log logger,
	sendBufSize, maxBatchBytes int,
	reconnectBase, reconnectMax time.Duration,
	reconnectJitter float64,
	subscribeChannels []string,
	publishChannels []string,
) *Client {
	stopCtx, stopCancel := context.WithCancel(context.Background())
	return &Client{
		instanceName:      instanceName,
		tlsCfg:            tlsCfg,
		policy:            policy,
		localWriter:       localWriter,
		dedup:             dedup,
		auditL:            auditL,
		log:               log,
		sendBufSize:       sendBufSize,
		maxBatchBytes:     maxBatchBytes,
		reconnectBase:     reconnectBase,
		reconnectMax:      reconnectMax,
		reconnectJitter:   reconnectJitter,
		subscribeChannels: subscribeChannels,
		publishChannels:   publishChannels,
		stop:              make(chan struct{}),
		stopCtx:           stopCtx,
		stopCancel:        stopCancel,
	}
}

// Dial connects to hubAddr once, exchanges the handshake, and starts goroutines.
// Returns the PeerSender the caller can use to enqueue outbound messages.
func (c *Client) Dial(hubAddr string) (*PeerSender, error) {
	return c.dial(hubAddr)
}

func (c *Client) dial(hubAddr string) (*PeerSender, error) {
	conn, err := c.getOrCreateGRPCConn(hubAddr)
	if err != nil {
		return nil, fmt.Errorf("federation: client grpc connect %s: %w", hubAddr, err)
	}

	stub := federationv1.NewFederationServiceClient(conn)
	outMD := metadata.Pairs("x-federation-instance", c.instanceName)

	// Open the Publish stream.
	pubCtx, pubCancel := context.WithCancel(c.stopCtx)
	pubStream, err := stub.Publish(metadata.NewOutgoingContext(pubCtx, outMD))
	if err != nil {
		pubCancel()
		return nil, fmt.Errorf("federation: client open publish stream %s: %w", hubAddr, err)
	}

	sender := NewPeerSender(pubStream, pubCancel, c.sendBufSize, c.maxBatchBytes, c.log)

	// Open the Subscribe stream if this client subscribes to channels.
	var receiver *PeerReceiver
	if c.localWriter != nil && len(c.subscribeChannels) > 0 {
		subCtx, subCancel := context.WithCancel(c.stopCtx)
		subStream, err := stub.Subscribe(metadata.NewOutgoingContext(subCtx, outMD))
		if err != nil {
			subCancel()
			sender.Close()
			return nil, fmt.Errorf("federation: client open subscribe stream %s: %w", hubAddr, err)
		}

		// Send the SubscribeRequest as the first frame on the Subscribe stream.
		if err := subStream.Send(&federationv1.SubscribeFrame{
			Payload: &federationv1.SubscribeFrame_Request{
				Request: &federationv1.SubscribeRequest{
					InstanceName: c.instanceName,
					Version:      wireVersion,
					Subscribe:    c.subscribeChannels,
				},
			},
		}); err != nil {
			subCancel()
			sender.Close()
			return nil, fmt.Errorf("federation: client send subscribe request %s: %w", hubAddr, err)
		}

		receiver = NewPeerReceiver(subStream, subCancel, c.policy, c.dedup, c.localWriter,
			c.auditL, c.log, hubAddr, c.maxBatchBytes)
	}

	c.mu.Lock()
	oldReceiver := c.receiver
	c.sender = sender
	c.receiver = receiver
	c.mu.Unlock()

	// Close the old receiver outside the lock; on reconnect the underlying stream
	// is already dead so this returns promptly.
	if oldReceiver != nil {
		oldReceiver.Close()
	}

	_ = c.auditL.Log(audit.Event{Event: audit.EventPeerConnected, PeerAddr: hubAddr})
	return sender, nil
}

// getOrCreateGRPCConn returns the existing gRPC connection or creates a new one
// targeting hubAddr. The connection is reused across reconnects.
func (c *Client) getOrCreateGRPCConn(hubAddr string) (*grpc.ClientConn, error) {
	c.grpcConnMu.Lock()
	defer c.grpcConnMu.Unlock()
	if c.grpcConn != nil {
		return c.grpcConn, nil
	}
	conn, err := newGRPCClientConn(hubAddr, c.tlsCfg)
	if err != nil {
		return nil, err
	}
	c.grpcConn = conn
	return conn, nil
}

// ConnectWithReconnect dials hubAddr and reconnects automatically on disconnect.
// It returns after the first successful connection.
func (c *Client) ConnectWithReconnect(hubAddr string) error {
	sender, err := c.dial(hubAddr)
	if err != nil {
		return err
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		backoff := c.reconnectBase
		attempt := 0
		for {
			select {
			case <-c.stop:
				return
			case <-sender.Done():
			}

			unacked := sender.Unacked()

			disconnDetail := fmt.Sprintf("unacked=%d", len(unacked))
			_ = c.auditL.Log(audit.Event{
				Event:    audit.EventPeerDisconnected,
				PeerAddr: hubAddr,
				Detail:   disconnDetail,
			})

			c.log.Warn("federation: client disconnected, reconnecting",
				"hub", hubAddr, "unacked", len(unacked))

			//nolint:gosec // G404: math/rand is appropriate for non-cryptographic jitter
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

			var newSender *PeerSender
			var dialErr error
			for {
				select {
				case <-c.stop:
					return
				default:
				}
				attempt++
				newSender, dialErr = c.dial(hubAddr)
				if dialErr == nil {
					break
				}
				if c.stopCtx.Err() != nil {
					return
				}
				c.log.Error("federation: client reconnect failed", "err", dialErr, "attempt", attempt)
				_ = c.auditL.Log(audit.Event{
					Event:    audit.EventPeerConnected,
					PeerAddr: hubAddr,
					Detail:   fmt.Sprintf("attempt=%d err=%s", attempt, dialErr.Error()),
				})
				backoff = minDuration(backoff*2, c.reconnectMax)
				//nolint:gosec // G404: math/rand is appropriate for non-cryptographic jitter
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

			for _, env := range unacked {
				newSender.Enqueue(env)
			}

			backoff = c.reconnectBase
			attempt = 0
			sender = newSender
		}
	}()
	return nil
}

// Sender returns the currently active PeerSender, or nil if not connected.
func (c *Client) Sender() *PeerSender {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sender
}

// AllowPublish reports whether the given channel is allowed to be published to the hub.
func (c *Client) AllowPublish(channel string) bool {
	for _, ch := range c.publishChannels {
		if ch == channel {
			return true
		}
	}
	return false
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
	if c.sender != nil {
		c.sender.Close()
	}
	if c.receiver != nil {
		c.receiver.Close()
	}
	c.mu.Unlock()
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

// newGRPCClientConn creates a gRPC client connection to target. Uses TLS when
// tlsCfg is non-nil, otherwise uses insecure (plaintext) credentials.
// The connection is created with lazy dialing; the actual TCP connection is
// established on the first RPC call.
func newGRPCClientConn(target string, tlsCfg *tls.Config) (*grpc.ClientConn, error) {
	var creds credentials.TransportCredentials
	if tlsCfg != nil {
		creds = credentials.NewTLS(tlsCfg)
	} else {
		creds = insecure.NewCredentials()
	}
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
}
