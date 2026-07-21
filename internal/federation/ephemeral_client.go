package federation

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
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

// connErrBox records the terminal error that ended one connection (nil until
// the connection drops). It is written by the per-connection goroutines and
// read by the reconnect loop after the connection's lost-signal fires, so the
// loop can distinguish a permanent failure (e.g. an allowlist rejection) from a
// transient disconnect. First non-nil write wins.
type connErrBox struct {
	mu  sync.Mutex
	err error
}

func (b *connErrBox) set(err error) {
	if err == nil {
		return
	}
	b.mu.Lock()
	if b.err == nil {
		b.err = err
	}
	b.mu.Unlock()
}

func (b *connErrBox) get() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

// isFatalConnErr reports whether err is a non-retryable connection failure —
// one where reconnecting cannot succeed without operator action. The hub
// rejects a client whose certificate CN is not in its allowlist with
// PermissionDenied; a malformed/expired identity surfaces as Unauthenticated.
func isFatalConnErr(err error) bool {
	if err == nil {
		return false
	}
	code := status.Code(err)
	return code == codes.PermissionDenied || code == codes.Unauthenticated
}

// EphemeralClientConfig holds construction-time settings for EphemeralClient.
type EphemeralClientConfig struct {
	// InstanceName identifies this client to the hub (must match TLS CN).
	InstanceName string
	// TLSConfig is the mTLS configuration. nil means plain-text gRPC.
	TLSConfig *tls.Config
	// Subscribe is the list of channels to receive from the hub.
	Subscribe []string
	// MaxBatchBytes is the max record size. Default: 4194304 (4 MiB). Set to 0 to disable.
	MaxBatchBytes int
	// WriteQueueSize is the outbound buffer depth. Default: 256.
	WriteQueueSize int
	// ReconnectBase is the initial reconnect backoff. Default: 500ms.
	ReconnectBase time.Duration
	// ReconnectMax is the maximum reconnect backoff. Default: 60s.
	ReconnectMax time.Duration
	// ReconnectJitter is the fractional jitter on the backoff (0–1). Default: 0.2.
	ReconnectJitter float64
	// OnFatal, when set, is invoked once with a non-retryable connection error
	// (e.g. the hub rejecting this client's identity with PermissionDenied or
	// Unauthenticated). After it fires the reconnect loop stops rather than
	// retrying a permanent failure forever. Called from a background goroutine;
	// the callback must not block. Only consulted in the auto-reconnect path.
	OnFatal func(error)
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
	onFatal         func(error)

	handlersMu sync.RWMutex
	handlers   map[string][]func(*envelope.Envelope) error

	// writeQ feeds the per-connection write goroutine. The channel persists
	// across reconnects so that ConnectWithReconnect can resume delivery.
	writeQ chan ephemeralWriteItem

	stop       chan struct{}
	stopOnce   sync.Once
	baseCtx    context.Context
	baseCancel context.CancelFunc
	wg         sync.WaitGroup
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
		cfg.MaxBatchBytes = 4 * 1024 * 1024
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
	baseCtx, baseCancel := context.WithCancel(context.Background()) //nolint:gosec // cancel stored in struct and called in Close()
	return &EphemeralClient{
		instanceName:    cfg.InstanceName,
		tlsCfg:          cfg.TLSConfig,
		subscribe:       cfg.Subscribe,
		maxBatchBytes:   cfg.MaxBatchBytes,
		log:             log,
		reconnectBase:   cfg.ReconnectBase,
		reconnectMax:    cfg.ReconnectMax,
		reconnectJitter: cfg.ReconnectJitter,
		onFatal:         cfg.OnFatal,
		handlers:        make(map[string][]func(*envelope.Envelope) error),
		writeQ:          make(chan ephemeralWriteItem, cfg.WriteQueueSize),
		stop:            make(chan struct{}),
		baseCtx:         baseCtx,
		baseCancel:      baseCancel,
	}
}

// AddHandler registers fn to be called for inbound messages on channel.
// Thread-safe; may be called before or after Connect.
func (c *EphemeralClient) AddHandler(channel string, fn func(*envelope.Envelope) error) {
	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()
	c.handlers[channel] = append(c.handlers[channel], fn)
}

// Connect dials hubAddr once and starts the background goroutines.
func (c *EphemeralClient) Connect(ctx context.Context, hubAddr string) error {
	_, _, err := c.startConn(ctx, hubAddr)
	return err
}

// ConnectWithReconnect dials hubAddr and starts an auto-reconnect loop.
func (c *EphemeralClient) ConnectWithReconnect(ctx context.Context, hubAddr string) error {
	connLost, errBox, err := c.startConn(ctx, hubAddr)
	if err != nil {
		return err
	}
	c.wg.Add(1)
	// #nosec G118 -- background reconnect loop doesn't need request context
	go c.reconnectLoop(hubAddr, connLost, errBox)
	return nil
}

// Publish sends env to the hub and blocks until the hub acks or ctx is done.
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

// Close stops all background goroutines. Safe to call more than once.
func (c *EphemeralClient) Close() {
	c.stopOnce.Do(func() {
		close(c.stop)
		c.baseCancel() // cancel all active gRPC stream contexts to unblock blocked Recv calls
	})
	c.wg.Wait()
}

// startConn dials hubAddr, opens Publish (and optionally Subscribe) gRPC streams,
// and starts per-connection goroutines. Returns a channel closed when the
// connection is lost.
func (c *EphemeralClient) startConn(ctx context.Context, hubAddr string) (<-chan struct{}, *connErrBox, error) {
	// errBox captures the terminal error of this connection so the reconnect
	// loop can tell a permanent rejection from a transient drop.
	errBox := &connErrBox{}

	// Ephemeral clients dispatch received messages in-memory and never re-forward
	// them, so they need no send-side loop filtering and do not capture the hub CN.
	grpcConn, err := newGRPCClientConn(hubAddr, c.tlsCfg, c.maxBatchBytes, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("ephemeral: grpc connect %s: %w", hubAddr, err)
	}

	stub := federationv1.NewFederationServiceClient(grpcConn)
	outMD := metadata.Pairs("x-federation-instance", c.instanceName)

	// Propagate caller context cancellation (e.g. timeout, early cancel).
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	// connCtx is derived from baseCtx so Close() → baseCancel() terminates streams.
	// A goroutine bridges ctx → connCtx so the caller's deadline/cancel is respected too.
	connCtx, connCancel := context.WithCancel(c.baseCtx)
	go func() {
		select {
		case <-ctx.Done():
			connCancel()
		case <-connCtx.Done():
		}
	}()

	// Open the Publish stream.
	pubStream, err := stub.Publish(metadata.NewOutgoingContext(connCtx, outMD))
	if err != nil {
		connCancel()
		_ = grpcConn.Close()
		return nil, nil, fmt.Errorf("ephemeral: open publish stream %s: %w", hubAddr, err)
	}

	// ackCh carries PublishAck messages from the Publish stream to writeLoop.
	ackCh := make(chan *federationv1.PublishAck, 4)

	// connDead is closed when either the Publish stream ends or c.stop fires.
	connDead := make(chan struct{})

	// Publish-ack reader goroutine: drains PublishAck messages from pubStream
	// and forwards them to ackCh. Closes connDead when the stream ends.
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(connDead)
		defer connCancel()
		defer func() { _ = grpcConn.Close() }()
		for {
			ack, recvErr := pubStream.Recv()
			if recvErr != nil {
				// Record the terminal error (e.g. PermissionDenied when the hub
				// rejects this client) so the reconnect loop can classify it.
				errBox.set(recvErr)
				return
			}
			select {
			case ackCh <- ack:
			case <-c.stop:
				return
			}
		}
	}()

	// Start the Subscribe stream if the client has subscriptions.
	if len(c.subscribe) > 0 {
		subCtx, subCancel := context.WithCancel(connCtx)
		subStream, subErr := stub.Subscribe(metadata.NewOutgoingContext(subCtx, outMD))
		if subErr == nil {
			subErr = subStream.Send(&federationv1.SubscribeFrame{
				Payload: &federationv1.SubscribeFrame_Request{
					Request: &federationv1.SubscribeRequest{
						Version:   wireVersion,
						Subscribe: c.subscribe,
						Ephemeral: true,
					},
				},
			})
		}
		if subErr != nil {
			subCancel()
			c.log.Warn("ephemeral: subscribe stream setup failed", "hub", hubAddr, "err", subErr)
		} else {
			dd, _ := dedup.NewLRUDedup(1024)
			recv := NewPeerReceiver(subStream, subCancel, nil, dd, c.dispatchEnvelope,
				nil /* no batch writer: ephemeral dispatches in-memory */, noopAuditLogger{}, c.log, hubAddr, c.instanceName, c.maxBatchBytes)
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				select {
				case <-recv.Done():
					// Capture a subscribe-stream rejection (e.g. PermissionDenied)
					// so a subscribe-only client still surfaces a fatal error.
					errBox.set(recv.Err())
				case <-c.stop:
					recv.Close()
				}
			}()
		}
	}

	connLost := make(chan struct{})
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(connLost)
		c.writeLoop(pubStream, ackCh, connDead)
	}()

	return connLost, errBox, nil
}

// dispatchEnvelope calls all registered handlers for env.Channel in order.
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

// writeLoop is the outbound write goroutine for a single connection.
func (c *EphemeralClient) writeLoop(
	pubStream federationv1.FederationService_PublishClient,
	ackCh <-chan *federationv1.PublishAck,
	connDead <-chan struct{},
) {
	signalAll := func(items []ephemeralWriteItem, err error) {
		for _, item := range items {
			select {
			case item.doneCh <- err:
			default:
			}
		}
	}

	// marshalItem encodes one queued item; on marshal failure it signals the
	// waiter and returns ok=false so the caller drops it from the batch.
	marshalItem := func(item ephemeralWriteItem) ([]byte, bool) {
		data, merr := envelope.Marshal(*item.env)
		if merr != nil {
			c.log.Error("ephemeral: marshal envelope", "id", item.env.ID, "err", merr)
			select {
			case item.doneCh <- fmt.Errorf("ephemeral: marshal: %w", merr):
			default:
			}
			return nil, false
		}
		return data, true
	}

	// pending holds an item pulled from writeQ that did not fit in the previous
	// batch; it becomes the first item of the next batch (forward progress).
	// If the loop returns (stop/connection loss) before it is sent, signal its
	// waiter so the caller is not stranded — mirrors the in-flight batch on
	// connDead. The caller's Publish also unblocks on c.stop independently.
	var pending *ephemeralWriteItem
	defer func() {
		if pending != nil {
			select {
			case pending.doneCh <- ErrEphemeralConnLost:
			default:
			}
		}
	}()

	for {
		var first ephemeralWriteItem
		if pending != nil {
			first = *pending
			pending = nil
		} else {
			select {
			case <-c.stop:
				return
			case <-connDead:
				return
			case item := <-c.writeQ:
				first = item
			}
		}

		batch := make([]ephemeralWriteItem, 0, 1)
		records := make([][]byte, 0, 1)
		totalBytes := 0

		// Always include the first item even if it alone exceeds MaxBatchBytes:
		// forward progress matters, and an oversized record is rejected by the
		// gRPC frame limit on Send rather than wedging the queue.
		if data, ok := marshalItem(first); ok {
			batch = append(batch, first)
			records = append(records, data)
			totalBytes += len(data)
		}

		// Drain additional queued items into the batch, stopping before the
		// configured MaxBatchBytes is exceeded. The overflow item is carried to
		// the next iteration via pending so it is never dropped.
	drainQ:
		for {
			select {
			case item := <-c.writeQ:
				data, ok := marshalItem(item)
				if !ok {
					continue
				}
				if c.maxBatchBytes > 0 && len(records) > 0 && totalBytes+len(data) > c.maxBatchBytes {
					stash := item
					pending = &stash
					break drainQ
				}
				batch = append(batch, item)
				records = append(records, data)
				totalBytes += len(data)
			default:
				break drainQ
			}
		}

		if len(records) == 0 {
			continue
		}

		if sendErr := pubStream.Send(&federationv1.PublishBatch{Records: records}); sendErr != nil {
			signalAll(batch, ErrEphemeralConnLost)
			return
		}

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

// reconnectLoop watches connLost and redials hubAddr with exponential backoff.
// If the connection ended with a non-retryable error (e.g. the hub rejecting
// this client's identity), it invokes OnFatal and stops instead of retrying a
// permanent failure forever.
func (c *EphemeralClient) reconnectLoop(hubAddr string, connLost <-chan struct{}, errBox *connErrBox) {
	defer c.wg.Done()
	backoff := c.reconnectBase
	for {
		select {
		case <-c.stop:
			return
		case <-connLost:
		}

		// A permanent rejection cannot be fixed by retrying; surface it and stop.
		if errBox != nil {
			if termErr := errBox.get(); isFatalConnErr(termErr) {
				c.log.Error("ephemeral: fatal connection error, not reconnecting",
					"hub", hubAddr, "err", termErr)
				if c.onFatal != nil {
					c.onFatal(termErr)
				}
				return
			}
		}

		c.log.Warn("ephemeral: connection lost, reconnecting", "hub", hubAddr)

		for {
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

			var err error
			connLost, errBox, err = c.startConn(context.Background(), hubAddr)
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
type noopAuditLogger struct{}

func (noopAuditLogger) Log(audit.Event) error { return nil }
func (noopAuditLogger) Close() error          { return nil }
