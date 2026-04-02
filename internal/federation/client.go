package federation

import (
	"crypto/tls"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/keyop/keyop-messenger/internal/audit"
	"github.com/keyop/keyop-messenger/internal/envelope"
)

// Client dials a hub, runs a PeerSender for outbound messages, and reconnects
// with exponential backoff on disconnect. A PeerReceiver can be optionally
// started for hubs that also push messages back to the client.
type Client struct {
	instanceName  string
	tlsCfg        *tls.Config
	policy        *AtomicPolicy
	localWriter   func(*envelope.Envelope) error
	dedup         Deduplicator
	auditL        audit.AuditLogger
	log           logger
	sendBufSize   int
	maxBatchBytes int

	// Reconnect parameters.
	reconnectBase   time.Duration
	reconnectMax    time.Duration
	reconnectJitter float64 // fraction, e.g. 0.2

	mu     sync.Mutex
	sender *PeerSender

	stop chan struct{}
	wg   sync.WaitGroup
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
) *Client {
	return &Client{
		instanceName:    instanceName,
		tlsCfg:          tlsCfg,
		policy:          policy,
		localWriter:     localWriter,
		dedup:           dedup,
		auditL:          auditL,
		log:             log,
		sendBufSize:     sendBufSize,
		maxBatchBytes:   maxBatchBytes,
		reconnectBase:   reconnectBase,
		reconnectMax:    reconnectMax,
		reconnectJitter: reconnectJitter,
		stop:            make(chan struct{}),
	}
}

// Dial connects to hubAddr once, exchanges the handshake, and starts goroutines.
// Returns the PeerSender the caller can use to enqueue outbound messages.
// On disconnect, Done() fires; call Dial again or use ConnectWithReconnect.
func (c *Client) Dial(hubAddr string) (*PeerSender, error) {
	return c.dial(hubAddr, "")
}

func (c *Client) dial(hubAddr, lastID string) (*PeerSender, error) {
	dialer := websocket.Dialer{TLSClientConfig: c.tlsCfg}
	url := "wss://" + hubAddr
	if c.tlsCfg == nil {
		url = "ws://" + hubAddr
	}
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return nil, fmt.Errorf("federation: client dial %s: %w", hubAddr, err)
	}

	// Send handshake first.
	if err := SendHandshake(conn, HandshakeMsg{
		InstanceName: c.instanceName,
		Role:         "client",
		Version:      wireVersion,
		LastID:       lastID,
	}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("federation: client send handshake: %w", err)
	}
	// Receive hub's handshake.
	if _, err := ReceiveHandshake(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("federation: client receive handshake: %w", err)
	}

	sender := NewPeerSender(conn, c.sendBufSize, c.maxBatchBytes, c.log)

	// Optional receiver: if the hub will push messages to this client.
	if c.localWriter != nil {
		NewPeerReceiver(conn, c.policy, c.dedup, c.localWriter,
			c.auditL, c.log, hubAddr, c.maxBatchBytes)
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
	sender, err := c.dial(hubAddr, "")
	if err != nil {
		return err
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		backoff := c.reconnectBase
		for {
			select {
			case <-c.stop:
				return
			case <-sender.Done():
			}

			// Gather unacked for replay.
			unacked := sender.Unacked()
			lastID := sender.LastAckedID()

			c.log.Warn("federation: client disconnected, reconnecting",
				"hub", hubAddr, "unacked", len(unacked))

			// Wait with backoff + jitter.
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
			for attempts := 0; ; attempts++ {
				select {
				case <-c.stop:
					return
				default:
				}
				newSender, dialErr = c.dial(hubAddr, lastID)
				if dialErr == nil {
					break
				}
				c.log.Error("federation: client reconnect failed", "err", dialErr)
				backoff = min(backoff*2, c.reconnectMax)
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

			// Replay unacked messages on new sender.
			for _, env := range unacked {
				newSender.Enqueue(env)
			}

			backoff = c.reconnectBase // reset on success
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

// Close stops the reconnect loop and the current connection.
func (c *Client) Close() {
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	c.mu.Lock()
	if c.sender != nil {
		c.sender.Close()
	}
	c.mu.Unlock()
	c.wg.Wait()
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
