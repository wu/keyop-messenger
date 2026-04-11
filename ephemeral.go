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
type EphemeralConfig struct {
	// HubAddr is the host:port of the hub to connect to. Required.
	HubAddr string
	// InstanceName identifies this client in the hub's allowlist.
	// Must match the CN of the TLS certificate. Required.
	InstanceName string
	// Subscribe lists the channels to receive messages from the hub.
	// These are declared in the handshake; the hub may deliver a subset
	// based on its access control policy. Fixed at connect time.
	Subscribe []string
	// TLS holds paths to the client's certificate, key, and CA.
	// Leave zero-value to use plain-text ws:// (testing/trusted networks only).
	TLS TLSConfig
	// AutoReconnect enables automatic reconnection on disconnect.
	// When true, ConnectWithReconnect is used; when false, Connect is used.
	AutoReconnect bool
	// ReconnectBase is the initial reconnect backoff. Default: 500ms.
	ReconnectBase time.Duration
	// ReconnectMax is the maximum reconnect backoff. Default: 60s.
	ReconnectMax time.Duration
	// ReconnectJitter is the fractional backoff jitter (0–1). Default: 0.2.
	ReconnectJitter float64
	// MaxBatchBytes is the maximum WebSocket frame payload in bytes. Default: 65536.
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
	client *federation.EphemeralClient
	reg    registry.PayloadRegistry
}

// NewEphemeralMessenger constructs an EphemeralMessenger. Call Subscribe to
// register inbound handlers, then Connect to dial the hub.
func NewEphemeralMessenger(cfg EphemeralConfig, opts ...Option) (*EphemeralMessenger, error) {
	if cfg.HubAddr == "" {
		return nil, errors.New("ephemeral messenger: HubAddr is required")
	}
	if cfg.InstanceName == "" {
		return nil, errors.New("ephemeral messenger: InstanceName is required")
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

	fedCfg := federation.EphemeralClientConfig{
		InstanceName:    cfg.InstanceName,
		TLSConfig:       tlsCfg,
		Subscribe:       cfg.Subscribe,
		MaxBatchBytes:   cfg.MaxBatchBytes,
		WriteQueueSize:  cfg.WriteQueueSize,
		ReconnectBase:   cfg.ReconnectBase,
		ReconnectMax:    cfg.ReconnectMax,
		ReconnectJitter: cfg.ReconnectJitter,
	}

	return &EphemeralMessenger{
		cfg:    cfg,
		client: federation.NewEphemeralClient(fedCfg, o.logger),
		reg:    registry.New(o.logger),
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
	m.client.AddHandler(channel, func(env *envelope.Envelope) error {
		payload, _ := reg.Decode(env.PayloadType, env.Payload)
		handler(Message{
			ID:          env.ID,
			Channel:     env.Channel,
			Origin:      env.Origin,
			PayloadType: env.PayloadType,
			Payload:     payload,
			Timestamp:   env.Ts,
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
	env, err := envelope.NewEnvelope(channel, m.cfg.InstanceName, payloadType, payload)
	if err != nil {
		return fmt.Errorf("ephemeral publish: create envelope: %w", err)
	}
	// Stamp correlation ID from context if present
	env.CorrelationID = CorrelationIDFromContext(ctx)
	return m.client.Publish(ctx, &env)
}

// Close disconnects from the hub and stops all background goroutines.
// Pending Publish calls receive ErrEphemeralClosed. Safe to call more than once.
func (m *EphemeralMessenger) Close() error {
	m.client.Close()
	return nil
}
