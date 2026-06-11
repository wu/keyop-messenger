package federation

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/http2"

	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/envelope"
)

// Client dials a hub over HTTP/2, runs a PeerSender for outbound messages, and
// reconnects with exponential backoff on disconnect. A PeerReceiver can be
// optionally started for hubs that also push messages back to the client.
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
	subscribeChannels []string // channels this client wants to receive from the hub
	publishChannels   []string // channels this client is allowed to publish to the hub

	// Reconnect parameters.
	reconnectBase   time.Duration
	reconnectMax    time.Duration
	reconnectJitter float64 // fraction, e.g. 0.2

	mu     sync.Mutex
	sender *PeerSender

	stop       chan struct{}
	stopCtx    context.Context
	stopCancel context.CancelFunc
	wg         sync.WaitGroup
}

// NewClient constructs a Client that is ready to dial. Call Dial or
// ConnectWithReconnect to establish a connection.
// subscribeChannels is the list of channels to request from the hub; the hub
// may deliver a subset based on its access control policy.
// publishChannels is the list of channels the client is allowed to publish to the hub.
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
// On disconnect, Done() fires; call Dial again or use ConnectWithReconnect.
func (c *Client) Dial(hubAddr string) (*PeerSender, error) {
	return c.dial(hubAddr)
}

// buildHTTPClient returns an http.Client configured for HTTP/2 over TLS (when
// tlsCfg is non-nil) or h2c cleartext HTTP/2 (when tlsCfg is nil).
func (c *Client) buildHTTPClient() *http.Client {
	if c.tlsCfg != nil {
		return &http.Client{
			Transport: &http2.Transport{
				TLSClientConfig: c.tlsCfg,
			},
		}
	}
	// Cleartext HTTP/2 (h2c) — used in tests and plain-TCP deployments.
	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		},
	}
}

func (c *Client) dial(hubAddr string) (*PeerSender, error) {
	scheme := "https"
	if c.tlsCfg == nil {
		scheme = "http"
	}
	url := scheme + "://" + hubAddr

	// Create a pipe for the HTTP/2 request body (client→hub direction).
	pr, pw := io.Pipe()

	req, err := http.NewRequestWithContext(c.stopCtx, http.MethodPost, url, pr)
	if err != nil {
		_ = pr.Close()
		_ = pw.Close()
		return nil, fmt.Errorf("federation: client build request %s: %w", hubAddr, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = -1 // streaming body of unknown length

	// Marshal the client handshake before dialing so it can be written
	// concurrently with the HTTP/2 HEADERS exchange.
	hsData, err := json.Marshal(HandshakeMsg{
		InstanceName: c.instanceName,
		Role:         "client",
		Version:      wireVersion,
		Subscribe:    c.subscribeChannels,
	})
	if err != nil {
		_ = pr.Close()
		_ = pw.Close()
		return nil, fmt.Errorf("federation: client marshal handshake: %w", err)
	}

	httpClient := c.buildHTTPClient()

	// Write the handshake to the request body pipe concurrently with Do().
	// Do() reads from pr (via HTTP/2 DATA frames) once the server starts reading
	// the request body. The goroutine blocks on pw.Write until pr is drained.
	writeErrCh := make(chan error, 1)
	go func() {
		// Use a temporary write-only conn to frame the handshake correctly.
		tmpConn := NewConn(nil, pw, nil, nil)
		writeErrCh <- tmpConn.WriteMessage(MsgTypeJSON, hsData)
	}()

	// Do() blocks until the server sends HTTP response headers (200 OK).
	// This happens after the server reads and processes the client handshake.
	resp, err := httpClient.Do(req)
	if err != nil {
		_ = pw.Close()
		_ = pr.Close()
		<-writeErrCh
		return nil, fmt.Errorf("federation: client connect %s: %w", hubAddr, err)
	}

	if writeErr := <-writeErrCh; writeErr != nil {
		_ = pw.Close()
		_ = resp.Body.Close()
		return nil, fmt.Errorf("federation: client send handshake: %w", writeErr)
	}

	if resp.StatusCode != http.StatusOK {
		_ = pw.Close()
		_ = resp.Body.Close()
		return nil, fmt.Errorf("federation: client connect %s: unexpected status %d", hubAddr, resp.StatusCode)
	}

	// Build the full-duplex Conn: read from response body (hub→client),
	// write to request body pipe (client→hub).
	closeFn := func() error {
		_ = pw.Close()
		_ = resp.Body.Close()
		return nil
	}
	conn := NewConn(resp.Body, pw, &stringAddr{"tcp", hubAddr}, closeFn)

	// Receive hub's handshake.
	if _, err := ReceiveHandshake(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("federation: client receive handshake: %w", err)
	}

	// When the hub pushes messages back to this client both a PeerSender and a
	// PeerReceiver share the same conn. Route acks through an internal channel
	// so the PeerReceiver owns all reads and avoids a concurrent-read race.
	connWriteMu := &sync.Mutex{}
	var sender *PeerSender
	if c.localWriter != nil {
		ackCh := make(chan AckMsg, 4)
		newPeerReceiverWithAck(conn, connWriteMu, c.policy, c.dedup, c.localWriter,
			c.auditL, c.log, hubAddr, c.maxBatchBytes, ackCh)
		sender = newPeerSenderWithAck(conn, connWriteMu, c.sendBufSize, c.maxBatchBytes, c.log, ackCh)
	} else {
		sender = NewPeerSender(conn, connWriteMu, c.sendBufSize, c.maxBatchBytes, c.log)
	}

	c.mu.Lock()
	c.sender = sender
	c.mu.Unlock()

	_ = c.auditL.Log(audit.Event{Event: audit.EventPeerConnected, PeerAddr: hubAddr})
	return sender, nil
}

// ConnectWithReconnect dials hubAddr and reconnects automatically on disconnect.
// It returns after the first successful connection. Subsequent reconnects happen
// in the background. Use Sender() to get the current PeerSender.
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

			// Gather unacked outbound messages for replay to hub.
			unacked := sender.Unacked()

			// Audit the disconnect so the cause is visible in the log.
			disconnDetail := fmt.Sprintf("unacked=%d", len(unacked))
			_ = c.auditL.Log(audit.Event{
				Event:    audit.EventPeerDisconnected,
				PeerAddr: hubAddr,
				Detail:   disconnDetail,
			})

			c.log.Warn("federation: client disconnected, reconnecting",
				"hub", hubAddr, "unacked", len(unacked))

			// Wait with backoff + jitter.
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
					return // shutdown cancelled the dial; exit without logging
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

			// Replay unacked outbound messages on new sender.
			for _, env := range unacked {
				newSender.Enqueue(env)
			}

			backoff = c.reconnectBase // reset on success
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
// An empty publishChannels list means the client does not publish to the hub at all.
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
	c.stopCancel() // cancels any in-flight httpClient.Do in dial()
	c.mu.Lock()
	if c.sender != nil {
		c.sender.Close()
	}
	c.mu.Unlock()
	c.wg.Wait()
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
