package federation

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"

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
	_, err := c.startConn(ctx, hubAddr)
	return err
}

// ConnectWithReconnect dials hubAddr and starts an auto-reconnect loop.
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
func (c *EphemeralClient) startConn(ctx context.Context, hubAddr string) (<-chan struct{}, error) {
	grpcConn, err := newGRPCClientConn(hubAddr, c.tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("ephemeral: grpc connect %s: %w", hubAddr, err)
	}

	stub := federationv1.NewFederationServiceClient(grpcConn)
	outMD := metadata.Pairs("x-federation-instance", c.instanceName)

	// Propagate caller context cancellation (e.g. timeout, early cancel).
	if err := ctx.Err(); err != nil {
		return nil, err
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
		return nil, fmt.Errorf("ephemeral: open publish stream %s: %w", hubAddr, err)
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
		subStream, subErr := stub.Subscribe(metadata.NewOutgoingContext(connCtx, outMD))
		if subErr == nil {
			subErr = subStream.Send(&federationv1.SubscribeFrame{
				Payload: &federationv1.SubscribeFrame_Request{
					Request: &federationv1.SubscribeRequest{
						InstanceName: c.instanceName,
						Version:      wireVersion,
						Subscribe:    c.subscribe,
						Ephemeral:    true,
					},
				},
			})
		}
		if subErr != nil {
			c.log.Warn("ephemeral: subscribe stream setup failed", "hub", hubAddr, "err", subErr)
		} else {
			dd, _ := dedup.NewLRUDedup(1024)
			_, subCancel := context.WithCancel(connCtx)
			recv := NewPeerReceiver(subStream, subCancel, nil, dd, c.dispatchEnvelope,
				noopAuditLogger{}, c.log, hubAddr, c.maxBatchBytes)
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				select {
				case <-recv.Done():
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

	return connLost, nil
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

	for {
		var first ephemeralWriteItem
		select {
		case <-c.stop:
			return
		case <-connDead:
			return
		case item := <-c.writeQ:
			first = item
		}

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
type noopAuditLogger struct{}

func (noopAuditLogger) Log(audit.Event) error { return nil }
func (noopAuditLogger) Close() error          { return nil }
