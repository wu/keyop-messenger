package messenger

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/federation"
	"github.com/wu/keyop-messenger/internal/registry"
	"github.com/wu/keyop-messenger/internal/tlsutil"
)

// ErrEphemeralConnLost is returned by EphemeralMessenger.Publish when the
// connection drops before the hub has acknowledged the message.
var ErrEphemeralConnLost = federation.ErrEphemeralConnLost

// EphemeralConfig configures an EphemeralMessenger.
//
// Instance identity is not configurable: it is derived from the local TLS
// certificate's Common Name. See [NewEphemeralMessenger].
type EphemeralConfig struct {
	// HubAddr is the host:port of the hub to connect to. Required.
	HubAddr string
	// Subscribe lists the channels to receive messages from the hub.
	// These are declared in the handshake; the hub may deliver a subset
	// based on its access control policy. Fixed at connect time.
	Subscribe []string
	// TLS holds paths to the client's certificate, key, and CA.
	// Leave zero-value to use plain-text ws:// (testing/trusted networks only).
	TLS TLSConfig
	// AutoReconnect enables automatic reconnection on disconnect.
	// When true, ConnectWithReconnect is used; when false, Connect is used.
	// When true, OnFatal is required: NewEphemeralMessenger returns an error if
	// AutoReconnect is set without an OnFatal handler.
	AutoReconnect bool
	// ReconnectBase is the initial reconnect backoff. Default: 500ms.
	ReconnectBase time.Duration
	// ReconnectMax is the maximum reconnect backoff. Default: 60s.
	ReconnectMax time.Duration
	// ReconnectJitter is the fractional backoff jitter (0–1). Default: 0.2.
	ReconnectJitter float64
	// OnFatal, when set, is invoked once with a non-retryable connection error —
	// most importantly the hub rejecting this client because its certificate CN
	// is not in the hub's allowlist (gRPC PermissionDenied), or an unauthenticated
	// identity (Unauthenticated). Because the connection is established lazily and
	// asynchronously, such a rejection cannot be returned from Connect; it is
	// delivered here instead, and the client stops retrying rather than looping on
	// a permanent failure. The callback runs on a background goroutine and must not
	// block. Only consulted when AutoReconnect is true — and required in that
	// case (see AutoReconnect).
	OnFatal func(error)
	// MaxBatchBytes is the maximum WebSocket frame payload in bytes. Default: 4194304 (4 MiB). Set to 0 to disable.
	MaxBatchBytes int
	// WriteQueueSize is the outbound buffer depth. Default: 256.
	WriteQueueSize int
}

// EphemeralMessenger connects to a hub without maintaining local state.
//
// Publish blocks until the hub acknowledges each message. If the connection
// drops before the ack arrives, Publish returns ErrEphemeralConnLost and the
// message may or may not have been received by the hub.
//
// Subscribe registers in-memory handlers that receive messages only while
// connected. On reconnect, delivery resumes from the current hub position;
// messages published while disconnected are never replayed.
//
// Construct with NewEphemeralMessenger; call Subscribe before Connect.
type EphemeralMessenger struct {
	cfg    EphemeralConfig
	name   string // derived from local TLS cert CN (or test override)
	client *federation.EphemeralClient
	reg    registry.PayloadRegistry
	log    Logger
}

// NewEphemeralMessenger constructs an EphemeralMessenger. Call Subscribe to
// register inbound handlers, then Connect to dial the hub.
func NewEphemeralMessenger(cfg EphemeralConfig, opts ...Option) (*EphemeralMessenger, error) {
	if cfg.HubAddr == "" {
		return nil, errors.New("ephemeral messenger: HubAddr is required")
	}
	// With AutoReconnect, a non-retryable rejection (e.g. this client's cert not
	// being in the hub's allowlist) is delivered only via OnFatal — nothing else
	// surfaces it, and the loop would otherwise retry a permanent failure forever.
	// Require OnFatal so that failure mode cannot be silently ignored.
	if cfg.AutoReconnect && cfg.OnFatal == nil {
		return nil, errors.New("ephemeral messenger: OnFatal is required when AutoReconnect is enabled")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	var tlsCfg *tls.Config
	if cfg.TLS.Cert != "" {
		var err error
		tlsCfg, err = tlsutil.BuildTLSConfig(cfg.TLS.Cert, cfg.TLS.Key, cfg.TLS.CA, o.logger)
		if err != nil {
			return nil, fmt.Errorf("ephemeral messenger: build TLS: %w", err)
		}
	}

	// Resolve instance identity. See [New] for the rationale; the rule is the
	// same here: production code derives the name from the local cert CN, and
	// tests may override with WithTestIdentity.
	var name string
	switch {
	case o.testIdentity != "":
		name = o.testIdentity
	case tlsCfg != nil:
		var err error
		name, err = tlsutil.ExtractLocalCN(tlsCfg)
		if err != nil {
			return nil, fmt.Errorf("ephemeral messenger: derive instance identity: %w", err)
		}
	default:
		return nil, errors.New("ephemeral messenger: TLS configuration required to determine instance identity")
	}

	fedCfg := federation.EphemeralClientConfig{
		InstanceName:    name,
		TLSConfig:       tlsCfg,
		Subscribe:       cfg.Subscribe,
		MaxBatchBytes:   cfg.MaxBatchBytes,
		WriteQueueSize:  cfg.WriteQueueSize,
		ReconnectBase:   cfg.ReconnectBase,
		ReconnectMax:    cfg.ReconnectMax,
		ReconnectJitter: cfg.ReconnectJitter,
		OnFatal:         cfg.OnFatal,
	}

	return &EphemeralMessenger{
		cfg:    cfg,
		name:   name,
		client: federation.NewEphemeralClient(fedCfg, o.logger),
		reg:    registry.New(),
		log:    o.logger,
	}, nil
}

// RegisterPayloadType associates typeStr with the Go type of prototype for
// decoding inbound message payloads delivered to Subscribe handlers.
// Registering the same typeStr twice returns ErrPayloadTypeAlreadyRegistered.
func (m *EphemeralMessenger) RegisterPayloadType(typeStr string, prototype any) error {
	if err := m.reg.Register(typeStr, prototype); err != nil {
		if errors.Is(err, registry.ErrPayloadTypeAlreadyRegistered) {
			return fmt.Errorf("%w: %q", ErrPayloadTypeAlreadyRegistered, typeStr)
		}
		return err
	}
	return nil
}

// Subscribe registers handler for inbound messages on channel.
// Should be called before Connect so the channel is included in the handshake.
// Handler is called synchronously within the receive goroutine; it must not block.
func (m *EphemeralMessenger) Subscribe(channel string, handler func(msg Message)) error {
	if err := ValidateChannelName(channel); err != nil {
		return err
	}
	reg := m.reg
	log := m.log
	m.client.AddHandler(channel, func(env *envelope.Envelope) error {
		payload, err := reg.Decode(env.PayloadType, env.Payload)
		if err != nil {
			// Unregistered or undecodable payload: skip delivery and warn. The
			// ephemeral path has no dead-letter queue, so there is nothing durable
			// to fall back to — but silently delivering a nil payload would hide a
			// registration mistake.
			log.Warn("ephemeral: skipping undecodable message",
				"channel", env.Channel, "type", env.PayloadType, "id", env.ID, "err", err)
			return nil
		}
		handler(Message{
			ID:            env.ID,
			Channel:       env.Channel,
			Origin:        env.Origin,
			PayloadType:   env.PayloadType,
			CorrelationID: env.CorrelationID,
			ServiceName:   env.ServiceName,
			Payload:       payload,
			Timestamp:     env.Ts,
		})
		return nil
	})
	return nil
}

// Connect dials the hub and starts the background goroutines.
//
// With AutoReconnect true: returns after the first connection; subsequent
// reconnects happen transparently in the background until Close is called.
//
// With AutoReconnect false: returns after the initial connection. On
// disconnect, subsequent Publish calls return ErrEphemeralConnLost until
// Connect is called again.
func (m *EphemeralMessenger) Connect(ctx context.Context) error {
	if m.cfg.AutoReconnect {
		return m.client.ConnectWithReconnect(ctx, m.cfg.HubAddr)
	}
	return m.client.Connect(ctx, m.cfg.HubAddr)
}

// Publish creates an envelope and blocks until the hub acknowledges it.
// Returns nil on success, ErrEphemeralConnLost if the connection drops before
// the ack, or a wrapped ctx.Err() if the context is cancelled.
func (m *EphemeralMessenger) Publish(ctx context.Context, channel, payloadType string, payload any) error {
	if err := ValidateChannelName(channel); err != nil {
		return err
	}
	env, err := envelope.NewEnvelope(channel, m.name, payloadType, payload)
	if err != nil {
		return fmt.Errorf("ephemeral publish: create envelope: %w", err)
	}
	// Stamp correlation ID and service name from context if present
	env.CorrelationID = CorrelationIDFromContext(ctx)
	env.ServiceName = ServiceNameFromContext(ctx)
	return m.client.Publish(ctx, &env)
}

// Close disconnects from the hub and stops all background goroutines.
// Pending Publish calls receive ErrEphemeralClosed. Safe to call more than once.
func (m *EphemeralMessenger) Close() error {
	m.client.Close()
	return nil
}
