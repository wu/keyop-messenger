package federation

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
)

// Sentinel errors returned by EphemeralClient.
var (
	// ErrEphemeralClosed is returned when an operation is attempted after Close.
	ErrEphemeralClosed = errors.New("ephemeral client closed")

	// ErrEphemeralConnLost is returned by Publish when the connection drops
	// before the hub has acknowledged the message.
	ErrEphemeralConnLost = errors.New("ephemeral client: connection lost before ack")
)

// EphemeralClientConfig holds construction-time settings for EphemeralClient.
type EphemeralClientConfig struct {
	// InstanceName identifies this client to the hub (must match TLS CN).
	InstanceName string
	// TLSConfig is the mTLS configuration. nil means plain-text ws://.
	TLSConfig *tls.Config
	// Subscribe is the list of channels to receive from the hub.
	// Sent in the handshake; the hub may deliver a subset per its policy.
	Subscribe []string
	// MaxBatchBytes is the max WebSocket frame payload size. Default: 4194304 (4 MiB). Set to 0 to disable.
	MaxBatchBytes int
	// WriteQueueSize is the outbound buffer depth. Default: 256.
	WriteQueueSize int
	// ReconnectBase is the initial reconnect backoff. Default: 500ms.
	ReconnectBase time.Duration
	// ReconnectMax is the maximum reconnect backoff. Default: 60s.
	ReconnectMax time.Duration
	// ReconnectJitter is the fractional jitter on the backoff (0–1). Default: 0.2.
	ReconnectJitter float64
}

// EphemeralClient connects to a hub without maintaining local state.
//
// Publish blocks until the hub acknowledges each message. If the connection
// drops before the ack arrives, Publish returns ErrEphemeralConnLost.
//
// Subscriptions are in-memory only: on reconnect the client starts receiving
// new messages from the current hub position with no replay of missed messages.
//
// Construct with NewEphemeralClient; call AddHandler before Connect.
type EphemeralClient struct {
	instanceName  string
	tlsCfg        *tls.Config
	subscribe     []string
	maxBatchBytes int
	log           logger

	reconnectBase   time.Duration
	reconnectMax    time.Duration
	reconnectJitter float64

	handlersMu sync.RWMutex
	handlers   map[string][]func(*envelope.Envelope) error

	// writeQ feeds the per-connection write goroutine. The channel persists
	// across reconnects so that ConnectWithReconnect can resume delivery.
	writeQ chan ephemeralWriteItem

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// ephemeralWriteItem is a pending Publish request waiting for a hub ack.
type ephemeralWriteItem struct {
	env    *envelope.Envelope
	doneCh chan error // buffered(1); signalled when acked or connection drops
}

// NewEphemeralClient constructs an EphemeralClient with defaults applied.
// Call AddHandler to register inbound message handlers, then Connect or
// ConnectWithReconnect to dial the hub.
func NewEphemeralClient(cfg EphemeralClientConfig, log logger) *EphemeralClient {
	if cfg.MaxBatchBytes == 0 {
		cfg.MaxBatchBytes = 4 * 1024 * 1024 // 4 MiB
	}
	if cfg.WriteQueueSize == 0 {
		cfg.WriteQueueSize = 256
	}
	if cfg.ReconnectBase == 0 {
		cfg.ReconnectBase = 500 * time.Millisecond
	}
	if cfg.ReconnectMax == 0 {
		cfg.ReconnectMax = 60 * time.Second
	}
	if cfg.ReconnectJitter == 0 {
		cfg.ReconnectJitter = 0.2
	}
	return &EphemeralClient{
		instanceName:    cfg.InstanceName,
		tlsCfg:          cfg.TLSConfig,
		subscribe:       cfg.Subscribe,
		maxBatchBytes:   cfg.MaxBatchBytes,
		log:             log,
		reconnectBase:   cfg.ReconnectBase,
		reconnectMax:    cfg.ReconnectMax,
		reconnectJitter: cfg.ReconnectJitter,
		handlers:        make(map[string][]func(*envelope.Envelope) error),
		writeQ:          make(chan ephemeralWriteItem, cfg.WriteQueueSize),
		stop:            make(chan struct{}),
	}
}

// AddHandler registers fn to be called for inbound messages on channel.
// Thread-safe; may be called before or after Connect. Only channels listed in
// EphemeralClientConfig.Subscribe will receive messages from the hub.
func (c *EphemeralClient) AddHandler(channel string, fn func(*envelope.Envelope) error) {
	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()
	c.handlers[channel] = append(c.handlers[channel], fn)
}

// Connect dials hubAddr once and starts the background goroutines.
// Returns after the connection is established. Disconnection is detected via
// subsequent Publish calls returning ErrEphemeralConnLost.
func (c *EphemeralClient) Connect(ctx context.Context, hubAddr string) error {
	_, err := c.startConn(ctx, hubAddr)
	return err
}

// ConnectWithReconnect dials hubAddr and starts an auto-reconnect loop.
// Returns after the first successful connection; subsequent reconnects happen
// in the background until Close is called.
func (c *EphemeralClient) ConnectWithReconnect(ctx context.Context, hubAddr string) error {
	connLost, err := c.startConn(ctx, hubAddr)
	if err != nil {
		return err
	}
	c.wg.Add(1)
	//nolint:gosec // G118: background reconnect loop doesn't need request context
	go c.reconnectLoop(hubAddr, connLost)
	return nil
}

// Publish sends env to the hub and blocks until the hub acks or ctx is done.
// Returns nil on success, ErrEphemeralConnLost if the connection drops first,
// or a wrapped ctx.Err() if the context is cancelled.
func (c *EphemeralClient) Publish(ctx context.Context, env *envelope.Envelope) error {
	item := ephemeralWriteItem{env: env, doneCh: make(chan error, 1)}
	select {
	case c.writeQ <- item:
	case <-c.stop:
		return ErrEphemeralClosed
	case <-ctx.Done():
		return fmt.Errorf("ephemeral publish: %w", ctx.Err())
	}
	select {
	case err := <-item.doneCh:
		return err
	case <-c.stop:
		return ErrEphemeralClosed
	case <-ctx.Done():
		return fmt.Errorf("ephemeral publish: %w", ctx.Err())
	}
}

// Close stops all background goroutines and closes the current connection.
// Pending Publish calls receive ErrEphemeralClosed. Safe to call more than once.
func (c *EphemeralClient) Close() {
	c.stopOnce.Do(func() { close(c.stop) })
	c.wg.Wait()
}

// startConn dials hubAddr, completes the ephemeral handshake, and starts the
// per-connection goroutines. Returns a channel closed when the connection is lost.
func (c *EphemeralClient) startConn(ctx context.Context, hubAddr string) (<-chan struct{}, error) {
	dialer := websocket.Dialer{TLSClientConfig: c.tlsCfg}
	wsURL := "wss://" + hubAddr
	if c.tlsCfg == nil {
		wsURL = "ws://" + hubAddr
	}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		// If context was cancelled, return context.Canceled to maintain error chain semantics
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("ephemeral: dial %s: %w", hubAddr, err)
	}

	if err := SendHandshake(conn, HandshakeMsg{
		InstanceName: c.instanceName,
		Role:         "client",
		Version:      wireVersion,
		Subscribe:    c.subscribe,
		Ephemeral:    true,
	}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ephemeral: send handshake: %w", err)
	}
	if _, err := ReceiveHandshake(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ephemeral: receive handshake: %w", err)
	}

	// ackCh: PeerReceiver routes text-frame acks (from the hub's PeerReceiver
	// acknowledging our published messages) here so the write goroutine can
	// unblock waiting Publish callers.
	ackCh := make(chan AckMsg, 4)

	connWriteMu := &sync.Mutex{}     // shared mutex for protecting concurrent writes to conn
	dd, _ := dedup.NewLRUDedup(1024) // small dedup for defense-in-depth
	receiver := newPeerReceiverWithAck(
		conn,
		connWriteMu,
		nil, // nil policy → accept all channels forwarded by the hub
		dd,
		c.dispatchEnvelope,
		noopAuditLogger{},
		c.log,
		hubAddr,
		c.maxBatchBytes,
		ackCh,
	)

	// connDead is closed when the PeerReceiver exits (read error or stop).
	// The write goroutine selects on it so it exits promptly even when blocked
	// waiting for new Publish calls rather than waiting for an ack.
	connDead := make(chan struct{})
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		select {
		case <-receiver.Done():
		case <-c.stop:
		}
		close(connDead)
		_ = conn.Close() // unblock any in-progress write; idempotent
		// Wait for the receiver to fully exit so Close() → wg.Wait() leaves
		// no dangling PeerReceiver goroutine.
		<-receiver.Done()
	}()

	connLost := make(chan struct{})
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(connLost)
		c.writeLoop(conn, ackCh, connDead)
	}()

	return connLost, nil
}

// dispatchEnvelope calls all registered handlers for env.Channel in order.
// Handler errors are logged but do not stop delivery to subsequent handlers.
func (c *EphemeralClient) dispatchEnvelope(env *envelope.Envelope) error {
	c.handlersMu.RLock()
	fns := c.handlers[env.Channel]
	c.handlersMu.RUnlock()
	for _, fn := range fns {
		if err := fn(env); err != nil {
			c.log.Warn("ephemeral: handler error",
				"channel", env.Channel, "id", env.ID, "err", err)
		}
	}
	return nil
}

// writeLoop is the outbound write goroutine for a single connection. It drains
// writeQ, writes batched binary frames, and blocks waiting for each hub ack.
// All items in a batch are signalled together: nil on ack, ErrEphemeralConnLost
// on connection loss, or ErrEphemeralClosed on Close.
func (c *EphemeralClient) writeLoop(conn *websocket.Conn, ackCh <-chan AckMsg, connDead <-chan struct{}) {
	signalAll := func(items []ephemeralWriteItem, err error) {
		for _, item := range items {
			select {
			case item.doneCh <- err:
			default:
			}
		}
	}

	for {
		// Block until the first item arrives, stop is signalled, or connection dies.
		var first ephemeralWriteItem
		select {
		case <-c.stop:
			return
		case <-connDead:
			return
		case item := <-c.writeQ:
			first = item
		}

		// Non-blocking drain: add any additional waiting items to the batch.
		batch := []ephemeralWriteItem{first}
	drainQ:
		for {
			select {
			case item := <-c.writeQ:
				batch = append(batch, item)
			default:
				break drainQ
			}
		}

		// Marshal each envelope. Items that fail to marshal are signalled with
		// an error immediately and excluded from the wire frame.
		var validBatch []ephemeralWriteItem
		records := make([][]byte, 0, len(batch))
		for _, item := range batch {
			data, merr := envelope.Marshal(*item.env)
			if merr != nil {
				c.log.Error("ephemeral: marshal envelope", "id", item.env.ID, "err", merr)
				select {
				case item.doneCh <- fmt.Errorf("ephemeral: marshal: %w", merr):
				default:
				}
				continue
			}
			validBatch = append(validBatch, item)
			records = append(records, data)
		}
		batch = validBatch
		if len(records) == 0 {
			continue
		}

		// Write one binary WebSocket frame containing all records in the batch.
		w, err := conn.NextWriter(websocket.BinaryMessage)
		if err != nil {
			signalAll(batch, ErrEphemeralConnLost)
			return
		}
		if err := WriteFrame(w, records); err != nil {
			_ = w.Close()
			signalAll(batch, ErrEphemeralConnLost)
			return
		}
		if err := w.Close(); err != nil {
			signalAll(batch, ErrEphemeralConnLost)
			return
		}

		// Wait for the hub's text-frame ack, routed here via ackCh by PeerReceiver.
		// The hub acks the entire batch; all waiting callers in the batch are unblocked.
		select {
		case <-c.stop:
			signalAll(batch, ErrEphemeralClosed)
			return
		case <-connDead:
			signalAll(batch, ErrEphemeralConnLost)
			return
		case _, ok := <-ackCh:
			if !ok {
				signalAll(batch, ErrEphemeralConnLost)
				return
			}
			signalAll(batch, nil)
		}
	}
}

// reconnectLoop watches connLost and redials hubAddr with exponential backoff
// until Close is called. It runs as a background goroutine started by
// ConnectWithReconnect.
func (c *EphemeralClient) reconnectLoop(hubAddr string, connLost <-chan struct{}) {
	defer c.wg.Done()
	backoff := c.reconnectBase
	for {
		select {
		case <-c.stop:
			return
		case <-connLost:
		}

		c.log.Warn("ephemeral: connection lost, reconnecting", "hub", hubAddr)

		for {
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

			var err error
			connLost, err = c.startConn(context.Background(), hubAddr)
			if err != nil {
				c.log.Error("ephemeral: reconnect failed", "hub", hubAddr, "err", err)
				backoff = min(backoff*2, c.reconnectMax)
				continue
			}
			backoff = c.reconnectBase
			c.log.Info("ephemeral: reconnected", "hub", hubAddr)
			break
		}
	}
}

// noopAuditLogger satisfies audit.AuditLogger by discarding all events.
// EphemeralClient has no audit log file.
type noopAuditLogger struct{}

func (noopAuditLogger) Log(audit.Event) error { return nil }
func (noopAuditLogger) Close() error          { return nil }
