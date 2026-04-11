package messenger

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/federation"
	"github.com/wu/keyop-messenger/internal/registry"
	"github.com/wu/keyop-messenger/internal/storage"
	"github.com/wu/keyop-messenger/internal/tlsutil"
)

// ----- Public errors ---------------------------------------------------------

var (
	// ErrPayloadTypeAlreadyRegistered is returned by RegisterPayloadType when the
	// same type string is registered more than once.
	ErrPayloadTypeAlreadyRegistered = errors.New("payload type already registered")

	// ErrInvalidChannelName is returned when a channel name is empty, exceeds
	// 255 bytes, or contains characters outside [a-zA-Z0-9._-].
	ErrInvalidChannelName = errors.New("invalid channel name")

	// ErrMessengerClosed is returned when an operation is attempted on a
	// Messenger after Close has been called.
	ErrMessengerClosed = errors.New("messenger is closed")
)

// ----- Channel name validation -----------------------------------------------

var channelNameRE = regexp.MustCompile(`^[a-zA-Z0-9._\-]+$`)

// ValidateChannelName returns a wrapped ErrInvalidChannelName if name is empty,
// exceeds 255 bytes, or contains characters outside [a-zA-Z0-9._-].
func ValidateChannelName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: name must not be empty", ErrInvalidChannelName)
	}
	if len(name) > 255 {
		return fmt.Errorf("%w: name exceeds 255 bytes", ErrInvalidChannelName)
	}
	if !channelNameRE.MatchString(name) {
		return fmt.Errorf("%w: %q contains characters outside [a-zA-Z0-9._-]", ErrInvalidChannelName, name)
	}
	return nil
}

// ----- Public types ----------------------------------------------------------

// Message is the decoded representation of a stored envelope delivered to a
// subscriber handler.
type Message struct {
	// ID is the UUID v4 that uniquely identifies this message.
	ID string
	// Channel is the channel the message was published to.
	Channel string
	// Origin is the instance name of the original publisher, preserved across
	// hub forwarding.
	Origin string
	// PayloadType is the type discriminator string (e.g. "com.acme.OrderCreated").
	PayloadType string
	// Payload is the decoded payload. Its concrete type is the prototype
	// registered via RegisterPayloadType, or map[string]any for unknown types.
	Payload any
	// Timestamp is the UTC time the message was published.
	Timestamp time.Time
}

// HandlerFunc processes a decoded Message. A non-nil return or a panic triggers
// the retry and dead-letter logic configured via subscribers.max_retries.
type HandlerFunc func(ctx context.Context, msg Message) error

// ----- Internal types --------------------------------------------------------

// subscriberEntry holds the runtime state for one active subscription.
type subscriberEntry struct {
	sub      *storage.Subscriber
	notifier *storage.LocalNotifier
	cancel   context.CancelFunc // cancels subCtx; wakes the ctx-watcher goroutine
}

// channelState holds all writers, subscribers, and compaction state for one
// channel. It is created on first access (Publish or Subscribe).
type channelState struct {
	writer    storage.ChannelWriter
	compactor *storage.Compactor
	mu        sync.RWMutex
	subs      map[string]*subscriberEntry
}

// broadcastNotify sends a non-blocking notification to every subscriber for
// this channel. Called from the channel writer's notifyFn goroutine.
func (cs *channelState) broadcastNotify() {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	for _, e := range cs.subs {
		e.notifier.Notify()
	}
}

// ----- Messenger -------------------------------------------------------------

// Messenger is the top-level pub-sub instance. Construct with New.
type Messenger struct {
	cfg     *Config
	log     Logger
	dataDir string

	reg    registry.PayloadRegistry
	dedup  *dedup.LRUDedup
	auditL audit.AuditLogger

	hub     *federation.Hub
	clients []*federation.Client

	mu       sync.RWMutex
	channels map[string]*channelState
	closed   bool

	stop      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// New constructs and starts a Messenger. It creates the required directory
// layout, initialises all internal components, and starts any configured hub
// listener and client connections.
func New(cfg *Config, opts ...Option) (*Messenger, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	if cfg == nil {
		if o.cfg == nil {
			return nil, errors.New("config is required")
		}
		cfg = o.cfg
	}

	if o.dataDir != "" {
		cfg.Storage.DataDir = o.dataDir
	}

	log := o.logger

	if err := EnsureDirectories(cfg); err != nil {
		return nil, err
	}

	reg := registry.New(log)

	dd, err := dedup.NewLRUDedup(cfg.Dedup.SeenIDLRUSize)
	if err != nil {
		return nil, fmt.Errorf("create dedup: %w", err)
	}

	auditL, err := audit.NewAuditWriter(
		filepath.Join(cfg.Storage.DataDir, "audit"),
		cfg.Audit.MaxSizeMB,
		cfg.Audit.MaxFiles,
		log,
	)
	if err != nil {
		return nil, fmt.Errorf("create audit writer: %w", err)
	}

	m := &Messenger{
		cfg:      cfg,
		log:      log,
		dataDir:  cfg.Storage.DataDir,
		reg:      reg,
		dedup:    dd,
		auditL:   auditL,
		channels: make(map[string]*channelState),
		stop:     make(chan struct{}),
	}

	// Build TLS config and check certificate expiry when certs are configured.
	var tlsCfg *tls.Config
	if cfg.TLS.Cert != "" {
		tlsCfg, err = tlsutil.BuildTLSConfig(cfg.TLS.Cert, cfg.TLS.Key, cfg.TLS.CA, log)
		if err != nil {
			return nil, fmt.Errorf("build TLS config: %w", err)
		}
		m.checkCertExpiry(tlsCfg, cfg)
	}

	// Start hub listener if enabled.
	if cfg.Hub.Enabled {
		hubCfg := toFedHubConfig(cfg.Hub)
		m.hub = federation.NewHub(
			cfg.Name,
			hubCfg,
			tlsCfg,
			m.writeLocalEnvelope,
			dd,
			auditL,
			log,
			cfg.Federation.SendBufferMessages,
			cfg.Federation.MaxBatchBytes,
			cfg.Storage.DataDir,
		)
		if err := m.hub.Listen(cfg.Hub.ListenAddr); err != nil {
			return nil, fmt.Errorf("start hub listener: %w", err)
		}
	}

	// Dial configured client hubs.
	if cfg.Client.Enabled {
		for _, ref := range cfg.Client.Hubs {
			policy := federation.NewAtomicPolicy(federation.ForwardPolicy{})
			c := federation.NewClient(
				cfg.Name,
				tlsCfg,
				policy,
				m.writeLocalEnvelope,
				dd,
				auditL,
				log,
				cfg.Federation.SendBufferMessages,
				cfg.Federation.MaxBatchBytes,
				time.Duration(cfg.Federation.ReconnectBaseMS)*time.Millisecond,
				time.Duration(cfg.Federation.ReconnectMaxMS)*time.Millisecond,
				cfg.Federation.ReconnectJitter,
				ref.Subscribe,
			)
			if err := c.ConnectWithReconnect(ref.Addr); err != nil {
				return nil, fmt.Errorf("connect to hub %q: %w", ref.Addr, err)
			}
			m.clients = append(m.clients, c)
		}
	}

	// Background compaction goroutine: periodically deletes consumed segments.
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.runCompaction()
			case <-m.stop:
				return
			}
		}
	}()

	return m, nil
}

// HubAddr returns the network address the hub is listening on, or empty string
// if no hub is running. Useful in tests to discover the dynamically-assigned port.
func (m *Messenger) HubAddr() string {
	if m.hub == nil {
		return ""
	}
	return m.hub.Addr()
}

// InstanceName returns the configured instance name for this messenger.
func (m *Messenger) InstanceName() string {
	return m.cfg.Name
}

// RegisterPayloadType associates typeStr with the Go type of prototype for
// decoding incoming message payloads. Registering the same typeStr twice
// returns ErrPayloadTypeAlreadyRegistered.
func (m *Messenger) RegisterPayloadType(typeStr string, prototype any) error {
	if err := m.reg.Register(typeStr, prototype); err != nil {
		if errors.Is(err, registry.ErrPayloadTypeAlreadyRegistered) {
			return fmt.Errorf("%w: %q", ErrPayloadTypeAlreadyRegistered, typeStr)
		}
		return err
	}
	return nil
}

// Publish creates an envelope for payload, writes it to channel's storage
// file, notifies local subscribers, and enqueues it for any configured peer
// senders. Publish blocks until the write is confirmed (per sync_policy).
func (m *Messenger) Publish(ctx context.Context, channel, payloadType string, payload any) error {
	if err := ValidateChannelName(channel); err != nil {
		return err
	}
	if m.isClosed() {
		return ErrMessengerClosed
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("publish %q: %w", channel, err)
	}

	cs, err := m.getOrCreateChannelState(channel)
	if err != nil {
		return fmt.Errorf("publish %q: %w", channel, err)
	}

	env, err := envelope.NewEnvelope(channel, m.cfg.Name, payloadType, payload)
	if err != nil {
		return fmt.Errorf("publish %q: create envelope: %w", channel, err)
	}

	// Mark in dedup so that if this message returns via federation it is
	// recognised as already seen and not re-delivered locally.
	m.dedup.SeenOrAdd(env.ID)

	if err := cs.writer.Write(&env); err != nil {
		return fmt.Errorf("publish %q: write: %w", channel, err)
	}

	// Enqueue to hub peer senders (hub's forward policy decides which peers).
	if m.hub != nil {
		m.hub.EnqueueToAll(&env)
	}
	// Enqueue to client senders (hub's receive policy decides acceptance).
	for _, c := range m.clients {
		if s := c.Sender(); s != nil {
			s.Enqueue(&env)
		}
	}

	return nil
}

// Subscribe registers handler for all new messages on channel. Delivery is
// at-least-once: the handler may be called again for the same message after a
// restart. The goroutine runs until ctx is cancelled or Unsubscribe is called.
func (m *Messenger) Subscribe(ctx context.Context, channel, subscriberID string, handler HandlerFunc) error {
	if err := ValidateChannelName(channel); err != nil {
		return err
	}
	if m.isClosed() {
		return ErrMessengerClosed
	}

	cs, err := m.getOrCreateChannelState(channel)
	if err != nil {
		return fmt.Errorf("subscribe %q: %w", channel, err)
	}

	// Ensure a dead-letter channel writer is ready.
	dlState, err := m.getOrCreateChannelState(channel + ".dead-letter")
	if err != nil {
		return fmt.Errorf("subscribe %q: dead-letter channel: %w", channel, err)
	}

	subCtx, cancel := context.WithCancel(ctx)

	notifier := storage.NewLocalNotifier()
	sub, err := storage.NewSubscriber(
		subscriberID,
		m.channelDir(channel),
		m.offsetDir(channel),
		m.reg,
		*m.cfg.Subscribers.MaxRetries,
		dlState.writer,
		m.log,
		time.Duration(m.cfg.Storage.OffsetFlushIntervalMS)*time.Millisecond,
	)
	if err != nil {
		cancel()
		return fmt.Errorf("subscribe %q/%q: %w", channel, subscriberID, err)
	}

	entry := &subscriberEntry{sub: sub, notifier: notifier, cancel: cancel}

	cs.mu.Lock()
	if _, exists := cs.subs[subscriberID]; exists {
		cs.mu.Unlock()
		cancel()
		return fmt.Errorf("subscribe %q/%q: already registered", channel, subscriberID)
	}
	cs.subs[subscriberID] = entry
	cs.compactor.RegisterSubscriber(subscriberID)
	cs.mu.Unlock()

	// Adapt the public HandlerFunc to the storage-level signature.
	storageHandler := func(env *envelope.Envelope, payload any) error {
		return handler(subCtx, Message{
			ID:          env.ID,
			Channel:     env.Channel,
			Origin:      env.Origin,
			PayloadType: env.PayloadType,
			Payload:     payload,
			Timestamp:   env.Ts,
		})
	}

	sub.Start(notifier.C(), storageHandler)

	// Ctx-watcher: stop the subscriber when subCtx is done or the Messenger
	// closes (whichever comes first).
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		select {
		case <-subCtx.Done():
			sub.Stop()
		case <-m.stop:
			sub.Stop()
		}
	}()

	return nil
}

// Unsubscribe stops the subscriber goroutine for (channel, subscriberID) and
// removes the offset file so the subscriber position is forgotten.
func (m *Messenger) Unsubscribe(channel, subscriberID string) error {
	if m.isClosed() {
		return ErrMessengerClosed
	}

	m.mu.RLock()
	cs, ok := m.channels[channel]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unsubscribe %q/%q: channel not found", channel, subscriberID)
	}

	cs.mu.Lock()
	entry, ok := cs.subs[subscriberID]
	if !ok {
		cs.mu.Unlock()
		return fmt.Errorf("unsubscribe %q/%q: subscriber not found", channel, subscriberID)
	}
	delete(cs.subs, subscriberID)
	cs.mu.Unlock()

	// cancel() wakes the ctx-watcher goroutine. Stop() blocks until the
	// subscriber goroutine exits (idempotent if already stopped).
	entry.cancel()
	entry.sub.Stop()
	// Remove the offset file so the subscriber position is forgotten.
	if err := cs.compactor.DeregisterSubscriber(subscriberID); err != nil {
		m.log.Warn("unsubscribe: remove offset file", "err", err)
	}

	return nil
}

// Close gracefully stops all subscribers, peer connections, and internal
// goroutines. Safe to call more than once.
func (m *Messenger) Close() error {
	var firstErr error
	m.closeOnce.Do(func() {
		// Mark as closed first so new Publish/Subscribe calls return ErrMessengerClosed.
		m.mu.Lock()
		m.closed = true
		m.mu.Unlock()

		// Signal all background goroutines (compaction, ctx-watchers).
		close(m.stop)

		// Explicitly stop all subscribers (belt-and-suspenders alongside ctx-watchers).
		m.mu.RLock()
		var entries []*subscriberEntry
		for _, cs := range m.channels {
			cs.mu.RLock()
			for _, e := range cs.subs {
				entries = append(entries, e)
			}
			cs.mu.RUnlock()
		}
		m.mu.RUnlock()
		for _, e := range entries {
			e.cancel()
			e.sub.Stop()
		}

		// Wait for all background goroutines to exit.
		m.wg.Wait()

		// Close hub (drains peer connections).
		if m.hub != nil {
			if err := m.hub.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}

		// Close client connections.
		for _, c := range m.clients {
			c.Close()
		}

		// Close all channel writers.
		m.mu.Lock()
		for _, cs := range m.channels {
			if err := cs.writer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		m.mu.Unlock()

		// Close audit writer last so it captures events from hub/client shutdown.
		if err := m.auditL.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	})
	return firstErr
}

// ----- Internal helpers ------------------------------------------------------

func (m *Messenger) isClosed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.closed
}

func (m *Messenger) channelDir(channel string) string {
	return filepath.Join(m.dataDir, "channels", channel)
}

func (m *Messenger) offsetDir(channel string) string {
	return filepath.Join(m.dataDir, "subscribers", channel)
}

// getOrCreateChannelState returns the existing channelState for channel or
// creates one (including a ChannelWriter and Compactor) under a write lock.
func (m *Messenger) getOrCreateChannelState(channel string) (*channelState, error) {
	m.mu.RLock()
	cs, ok := m.channels[channel]
	m.mu.RUnlock()
	if ok {
		return cs, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Double-check under write lock.
	if cs, ok = m.channels[channel]; ok {
		return cs, nil
	}

	cs = &channelState{
		subs: make(map[string]*subscriberEntry),
		compactor: storage.NewCompactor(
			m.offsetDir(channel),
			int64(m.cfg.Storage.MaxSubscriberLagMB)*1024*1024,
			m.log,
		),
	}

	// notifyFn is invoked by the writer goroutine after every successful write.
	// The closure captures cs by pointer so newly-added subscribers are notified.
	notifyFn := func() { cs.broadcastNotify() }

	var err error
	cs.writer, err = storage.NewChannelWriter(
		m.channelDir(channel),
		int64(m.cfg.Storage.CompactionThresholdMB)*1024*1024,
		storage.SyncPolicy(m.cfg.Storage.SyncPolicy),
		time.Duration(m.cfg.Storage.SyncIntervalMS)*time.Millisecond,
		notifyFn,
		m.log,
	)
	if err != nil {
		return nil, fmt.Errorf("create channel writer for %q: %w", channel, err)
	}

	m.channels[channel] = cs
	return cs, nil
}

// writeLocalEnvelope is the federation localWriter callback. It is called by
// PeerReceivers after deduplication to store a remotely-published envelope and
// wake local subscribers.
func (m *Messenger) writeLocalEnvelope(env *envelope.Envelope) error {
	cs, err := m.getOrCreateChannelState(env.Channel)
	if err != nil {
		return err
	}
	if err := cs.writer.Write(env); err != nil {
		return err
	}

	// Forward to all connected peers. All peers have senders created at connection
	// time (even those without subscriptions), so messages can be forwarded to them.
	if m.hub != nil {
		m.hub.EnqueueToAll(env)
	}

	return nil
}

// runCompaction iterates all known channels and attempts to delete fully-consumed
// sealed segments.
func (m *Messenger) runCompaction() {
	m.mu.RLock()
	channels := make(map[string]*channelState, len(m.channels))
	for k, v := range m.channels {
		channels[k] = v
	}
	m.mu.RUnlock()

	for ch, cs := range channels {
		if err := cs.compactor.MaybeCompact(m.channelDir(ch)); err != nil {
			m.log.Error("compaction failed", "channel", ch, "err", err)
		}
	}
}

// checkCertExpiry parses the loaded TLS certificates and logs expiry warnings.
func (m *Messenger) checkCertExpiry(tlsCfg *tls.Config, cfg *Config) {
	if len(tlsCfg.Certificates) > 0 {
		cert := tlsCfg.Certificates[0]
		if len(cert.Certificate) > 0 {
			if leaf, err := x509.ParseCertificate(cert.Certificate[0]); err == nil {
				tlsutil.CheckExpiry(leaf, cfg.TLS.ExpiryWarnDays, m.log)
			}
		}
	}
	if cfg.TLS.CA != "" {
		if caPEM, err := os.ReadFile(cfg.TLS.CA); err == nil {
			if block, _ := pem.Decode(caPEM); block != nil {
				if caCert, err := x509.ParseCertificate(block.Bytes); err == nil {
					tlsutil.CheckExpiry(caCert, cfg.TLS.ExpiryWarnDays, m.log)
				}
			}
		}
	}
}

// toFedHubConfig converts the messenger-level HubConfig to the federation
// package's HubConfig.
func toFedHubConfig(cfg HubConfig) federation.HubConfig {
	peers := make([]federation.AllowedPeer, len(cfg.AllowedPeers))
	for i, p := range cfg.AllowedPeers {
		peers[i] = federation.AllowedPeer{
			Name:      p.Name,
			Subscribe: p.Subscribe,
			Publish:   p.Publish,
		}
	}
	return federation.HubConfig{
		AllowedPeers: peers,
	}
}
