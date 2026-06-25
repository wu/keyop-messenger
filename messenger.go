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
	"strings"
	"sync"
	"sync/atomic"
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

	// ErrReservedChannelName is returned by Publish and PublishBatch when the
	// target channel uses the reserved ".dead-letter" suffix. Dead-letter
	// channels are written only by the messenger itself when a handler exhausts
	// its retry budget; applications may subscribe to them for monitoring or
	// reprocessing but must not publish to them directly.
	ErrReservedChannelName = errors.New("reserved channel name")

	// ErrMessengerClosed is returned when an operation is attempted on a
	// Messenger after Close has been called.
	ErrMessengerClosed = errors.New("messenger is closed")

	// ErrRetryLater signals a transient failure when returned (or wrapped) by a
	// HandlerFunc: the message is not dead-lettered and the offset is not
	// advanced, so the durable channel log buffers it until a later delivery
	// attempt succeeds. The message is re-attempted from the same offset on the
	// next notify/poll. Use it for downstream-unavailable conditions (e.g. a sink
	// server is down) where dropping the message is worse than pausing delivery.
	ErrRetryLater = storage.ErrRetryLater
)

// ----- Channel name validation -----------------------------------------------

// deadLetterSuffix is the reserved channel-name suffix for dead-letter channels.
// A subscriber routes a message to "{channel}.dead-letter" after its handler
// exhausts the retry budget. Applications must not publish to a channel with
// this suffix; see ErrReservedChannelName.
const deadLetterSuffix = ".dead-letter"

// defaultDeadLetterRetention bounds dead-letter channel growth when no global
// storage retention is configured. Failed messages are kept this long for
// inspection or reprocessing, after which the compactor force-deletes the
// oldest sealed segments. See channelRetention.
const defaultDeadLetterRetention = 7 * 24 * time.Hour

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
	// ID is the UUID v7 that uniquely identifies this message.
	ID string
	// Channel is the channel the message was published to.
	Channel string
	// Origin is the instance name of the original publisher, preserved across
	// hub forwarding.
	Origin string
	// PayloadType is the type discriminator string (e.g. "com.acme.OrderCreated").
	PayloadType string
	// CorrelationID is an optional application-level identifier used to group
	// related messages across a multi-step process.
	CorrelationID string
	// ServiceName is the name of the service that published this message.
	ServiceName string
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

// DiagnosticStats is a snapshot of all instrumentation counters for one
// (channel, subscriberID) pair. Used by tests and observability hooks to
// investigate lost-wakeup or offset-skew bugs.
type DiagnosticStats struct {
	NotifySent       int64 // successful Notify calls
	NotifyDropped    int64 // Notify calls dropped because notifyC was full
	WakesByNotify    int64 // subscriber select cases that fired on notifyC
	WakesByPoll      int64 // subscriber select cases that fired on the safety ticker
	ProcessCalls     int64 // total processAvailable invocations
	Dispatched       int64 // messages successfully passed to the handler
	UnmarshalSkipped int64 // lines that failed envelope.Unmarshal (offset still advanced)
	DecodeSkipped    int64 // payloads that failed reg.Decode (offset still advanced)
	OversizedSkipped int64 // records larger than maxLineSize (offset still advanced)
	CompactionDrops  int64 // messages permanently dropped by retention before delivery (data loss)
	RetryLaterPauses int64 // delivery pauses because the downstream handler asked to retry later (backpressure)
	StartupSkipped   int64 // bytes skipped on startup because the stored offset predated retention (informational)
	CurrentOffset    int64 // subscriber's in-memory cursor
	FlushedOffset    int64 // subscriber's on-disk cursor
}

// DiagnosticStats returns counters for the named subscription. Returns the
// zero value if the subscription doesn't exist (caller should check Dispatched
// to disambiguate "not found" from "exists but processed nothing").
func (m *Messenger) DiagnosticStats(channel, subscriberID string) DiagnosticStats {
	m.mu.RLock()
	cs, ok := m.channels[channel]
	m.mu.RUnlock()
	if !ok {
		return DiagnosticStats{}
	}
	cs.mu.RLock()
	entry, ok := cs.subs[subscriberID]
	cs.mu.RUnlock()
	if !ok {
		return DiagnosticStats{}
	}
	subStats := entry.sub.Stats()
	notifies, drops := entry.notifier.Stats()
	return DiagnosticStats{
		NotifySent:       notifies,
		NotifyDropped:    drops,
		WakesByNotify:    subStats.WakesByNotify,
		WakesByPoll:      subStats.WakesByPoll,
		ProcessCalls:     subStats.ProcessCalls,
		Dispatched:       subStats.Dispatched,
		UnmarshalSkipped: subStats.UnmarshalSkipped,
		DecodeSkipped:    subStats.DecodeSkipped,
		OversizedSkipped: subStats.OversizedSkipped,
		CompactionDrops:  subStats.CompactionDrops,
		RetryLaterPauses: subStats.RetryLaterPauses,
		StartupSkipped:   subStats.StartupSkippedBytes,
		CurrentOffset:    subStats.CurrentOffset,
		FlushedOffset:    subStats.FlushedOffset,
	}
}

// channelState holds all writers, subscribers, and compaction state for one
// channel. It is created on first access (Publish or Subscribe).
type channelState struct {
	writer       storage.ChannelWriter
	compactor    *storage.Compactor
	mu           sync.RWMutex
	subs         map[string]*subscriberEntry
	publishCount atomic.Int64
}

// countingWriter wraps a ChannelWriter to increment a publish counter on each
// successful write. It is used for the dead-letter writer handed to subscribers:
// their writes go straight to the writer and bypass the Publish path that
// normally maintains a channel's publishCount, so without this wrapper a
// dead-letter channel's MessageCount (and any total derived from it) would stay
// at zero. The federation path (writeLocalEnvelope) increments publishCount
// itself, so the dead-letter channelState's own writer is left unwrapped to
// avoid double counting.
type countingWriter struct {
	storage.ChannelWriter
	count *atomic.Int64
}

func (w countingWriter) Write(ctx context.Context, env *envelope.Envelope) error {
	if err := w.ChannelWriter.Write(ctx, env); err != nil {
		return err
	}
	w.count.Add(1)
	return nil
}

func (w countingWriter) WriteBatch(ctx context.Context, envs []*envelope.Envelope) error {
	if err := w.ChannelWriter.WriteBatch(ctx, envs); err != nil {
		return err
	}
	w.count.Add(int64(len(envs)))
	return nil
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
	name    string // derived from local TLS cert CN (or test override)
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

	// publish-to-disk latency aggregate (Latency.PublishToDisk): summed write
	// durations and the operation count, accumulated across every write site
	// (Publish, PublishBatch, federation writeLocalEnvelope).
	writeLatencySumNs atomic.Int64
	writeLatencyCount atomic.Int64
}

// recordWriteLatency adds one publish-to-disk sample (the elapsed time since
// start) to the instance-wide write-latency aggregate. Called after a writer
// Write/WriteBatch returns success.
func (m *Messenger) recordWriteLatency(start time.Time) {
	m.writeLatencySumNs.Add(int64(time.Since(start)))
	m.writeLatencyCount.Add(1)
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

	// If construction fails after this point, roll back the resources we have
	// already started. Otherwise the audit writer's background goroutine (and any
	// hub/client goroutines) leak and keep writing into the data dir, which on a
	// caller using t.TempDir() races RemoveAll and fails the test.
	var m *Messenger
	success := false
	defer func() {
		if success {
			return
		}
		if m != nil {
			if m.hub != nil {
				_ = m.hub.Close()
			}
			for _, c := range m.clients {
				c.Close()
			}
		}
		_ = auditL.Close()
	}()

	// Build TLS config and check certificate expiry when certs are configured.
	var tlsCfg *tls.Config
	if cfg.TLS.Cert != "" {
		tlsCfg, err = tlsutil.BuildTLSConfig(cfg.TLS.Cert, cfg.TLS.Key, cfg.TLS.CA, log)
		if err != nil {
			return nil, fmt.Errorf("build TLS config: %w", err)
		}
	}

	// Resolve instance identity. Federation (hub or client enabled) requires
	// TLS, and identity is the local cert's CN. Local-only instances may
	// omit TLS, in which case identity falls back to the OS hostname. The
	// test-only WithTestIdentity option bypasses both for tests.
	needFederation := cfg.Hub.Enabled || cfg.Client.Enabled
	var name string
	switch {
	case o.testIdentity != "":
		name = o.testIdentity
	case tlsCfg != nil:
		name, err = tlsutil.ExtractLocalCN(tlsCfg)
		if err != nil {
			return nil, fmt.Errorf("derive instance identity: %w", err)
		}
	case needFederation:
		return nil, errors.New("federation requires TLS configuration (set tls.cert/key/ca)")
	default:
		// Local-only with no TLS: identity is the OS hostname.
		name, err = os.Hostname()
		if err != nil || name == "" {
			return nil, errors.New("could not determine instance identity: no TLS configured and OS hostname unavailable")
		}
	}

	m = &Messenger{
		cfg:      cfg,
		name:     name,
		log:      log,
		dataDir:  cfg.Storage.DataDir,
		reg:      reg,
		dedup:    dd,
		auditL:   auditL,
		channels: make(map[string]*channelState),
		stop:     make(chan struct{}),
	}

	if tlsCfg != nil {
		m.checkCertExpiry(tlsCfg, cfg)
	}

	// Start hub listener if enabled.
	if cfg.Hub.Enabled {
		hubCfg := toFedHubConfig(cfg.Hub)
		m.hub = federation.NewHub(
			hubCfg,
			tlsCfg,
			m.writeLocalEnvelopeBatch,
			dd,
			auditL,
			log,
			cfg.Federation.SendBufferMessages,
			cfg.Federation.MaxBatchBytes,
			cfg.Storage.DataDir,
		)
		m.hub.SetFedClientOffsetTTL(cfg.Hub.FedClientOffsetTTL.Duration)
		if err := m.hub.Listen(cfg.Hub.ListenAddr); err != nil {
			return nil, fmt.Errorf("start hub listener: %w", err)
		}
	}

	// Dial configured client hubs.
	if cfg.Client.Enabled {
		for _, ref := range cfg.Client.Hubs {
			policy := federation.NewAtomicPolicy(federation.ForwardPolicy{
				Receive: ref.Subscribe, // channels client may receive from hub (checked by PeerReceiver)
			})
			c := federation.NewClient(
				m.name,
				tlsCfg,
				policy,
				m.writeLocalEnvelope,
				m.writeLocalEnvelopeBatch,
				dd,
				auditL,
				log,
				cfg.Federation.MaxBatchBytes,
				time.Duration(cfg.Federation.ReconnectBaseMS)*time.Millisecond,
				time.Duration(cfg.Federation.ReconnectMaxMS)*time.Millisecond,
				cfg.Federation.ReconnectJitter,
				ref.Subscribe,
				ref.Publish,
				cfg.Storage.DataDir,
			)
			if err := c.ConnectWithReconnect(ref.Addr); err != nil {
				c.Close()
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

	success = true
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

// InstanceName returns the instance identity for this messenger, derived from
// the local TLS certificate's Common Name.
func (m *Messenger) InstanceName() string {
	return m.name
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
// file, and notifies local subscribers and any configured outbound peer
// readers. Publish blocks until the write is confirmed (per sync_interval_ms).
// Outbound delivery to configured client hubs continues asynchronously from the
// channel file; a hub disconnect does not lose messages because each outbound
// reader tracks its own durable offset.
func (m *Messenger) Publish(ctx context.Context, channel, payloadType string, payload any) error {
	if err := ValidateChannelName(channel); err != nil {
		return err
	}
	if strings.HasSuffix(channel, deadLetterSuffix) {
		return fmt.Errorf("%w: %q: the %q suffix is reserved", ErrReservedChannelName, channel, deadLetterSuffix)
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

	env, err := envelope.NewEnvelope(channel, m.name, payloadType, payload)
	if err != nil {
		return fmt.Errorf("publish %q: create envelope: %w", channel, err)
	}

	// Stamp correlation ID and service name from context if present
	env.CorrelationID = CorrelationIDFromContext(ctx)
	env.ServiceName = ServiceNameFromContext(ctx)

	// Mark in dedup so that if this message returns via federation it is
	// recognised as already seen and not re-delivered locally.
	m.dedup.SeenOrAdd(env.ID)

	writeStart := time.Now()
	if err := cs.writer.Write(ctx, &env); err != nil {
		return fmt.Errorf("publish %q: write: %w", channel, err)
	}
	m.recordWriteLatency(writeStart)
	cs.publishCount.Add(1)

	// Notify hub channel readers that new data is available for this channel.
	if m.hub != nil {
		m.hub.NotifyChannel(env.Channel)
	}
	// Wake any outbound client readers configured for this channel. Each client
	// pulls from the local channel file via its channelReader, so the message
	// is durable through hub disconnects.
	for _, c := range m.clients {
		if c.AllowPublish(env.Channel) {
			c.NotifyChannel(env.Channel)
		}
	}

	return nil
}

// BatchMessage is one message in a PublishBatch. All messages in a batch are
// published to the same channel, but each carries its own payload type — a
// channel commonly carries several event types (alerts, metrics, status).
type BatchMessage struct {
	// PayloadType is the type discriminator for Payload, e.g.
	// "com.keyop.orders.OrderCreated". Reverse-DNS format is recommended.
	PayloadType string
	// Payload is any JSON-serialisable value.
	Payload any
}

// PublishBatch creates an envelope for each message and writes them all to
// channel's storage as a single durable unit: one fsync and one subscriber
// notification cover the whole batch (when sync_interval_ms=0), amortising the
// per-message fsync cost. Every message goes to the same channel but may carry
// a different payload type; message order is preserved.
//
// PublishBatch blocks until the whole batch is durably committed. The return
// contract matches Publish: nil means every message was committed; a non-nil
// error means the batch was not fully committed and may be safely retried. A
// retry may duplicate the record-aligned prefix that reached disk before a
// failure, consistent with at-least-once delivery. An empty batch is a no-op.
//
// Outbound delivery to configured client hubs continues asynchronously from the
// channel file, exactly as for Publish.
func (m *Messenger) PublishBatch(ctx context.Context, channel string, msgs []BatchMessage) error {
	if err := ValidateChannelName(channel); err != nil {
		return err
	}
	if strings.HasSuffix(channel, deadLetterSuffix) {
		return fmt.Errorf("%w: %q: the %q suffix is reserved", ErrReservedChannelName, channel, deadLetterSuffix)
	}
	if m.isClosed() {
		return ErrMessengerClosed
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("publish batch %q: %w", channel, err)
	}
	if len(msgs) == 0 {
		return nil
	}

	cs, err := m.getOrCreateChannelState(channel)
	if err != nil {
		return fmt.Errorf("publish batch %q: %w", channel, err)
	}

	// Correlation ID and service name are stamped once from the context and
	// shared by every envelope in the batch.
	correlationID := CorrelationIDFromContext(ctx)
	serviceName := ServiceNameFromContext(ctx)

	envs := make([]*envelope.Envelope, 0, len(msgs))
	for _, msg := range msgs {
		env, err := envelope.NewEnvelope(channel, m.name, msg.PayloadType, msg.Payload)
		if err != nil {
			return fmt.Errorf("publish batch %q: create envelope: %w", channel, err)
		}
		env.CorrelationID = correlationID
		env.ServiceName = serviceName
		// Mark in dedup so that if this message returns via federation it is
		// recognised as already seen and not re-delivered locally.
		m.dedup.SeenOrAdd(env.ID)
		envs = append(envs, &env)
	}

	writeStart := time.Now()
	if err := cs.writer.WriteBatch(ctx, envs); err != nil {
		return fmt.Errorf("publish batch %q: write: %w", channel, err)
	}
	m.recordWriteLatency(writeStart)
	cs.publishCount.Add(int64(len(envs)))

	// One notification per batch wakes local subscribers and outbound readers;
	// each drains all newly-available records on the next read.
	if m.hub != nil {
		m.hub.NotifyChannel(channel)
	}
	for _, c := range m.clients {
		if c.AllowPublish(channel) {
			c.NotifyChannel(channel)
		}
	}

	return nil
}

// Subscribe registers handler for all new messages on channel. Delivery is
// at-least-once: the handler may be called again for the same message after a
// restart. The goroutine runs until ctx is cancelled or Unsubscribe is called.
func (m *Messenger) Subscribe(ctx context.Context, channel, subscriberID string, handler HandlerFunc, opts ...SubscribeOption) error {
	if err := ValidateChannelName(channel); err != nil {
		return err
	}
	if m.isClosed() {
		return ErrMessengerClosed
	}

	var so subscribeOptions
	for _, opt := range opts {
		opt(&so)
	}

	// Effective retry policy: instance config by default, overridable per
	// subscription via WithMaxRetries / WithRetryBackoff.
	maxRetries := *m.cfg.Subscribers.MaxRetries
	if so.maxRetries != nil {
		maxRetries = *so.maxRetries
		if maxRetries < 0 {
			maxRetries = 0
		}
	}
	retryBase := time.Duration(m.cfg.Subscribers.RetryBackoffBaseMS) * time.Millisecond
	retryMax := time.Duration(m.cfg.Subscribers.RetryBackoffMaxMS) * time.Millisecond
	if so.retryBackoffSet {
		retryBase = so.retryBase
		retryMax = so.retryMax
	}

	cs, err := m.getOrCreateChannelState(channel)
	if err != nil {
		return fmt.Errorf("subscribe %q: %w", channel, err)
	}

	// Ensure a dead-letter channel writer is ready.
	dlState, err := m.getOrCreateChannelState(channel + deadLetterSuffix)
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
		maxRetries,
		countingWriter{ChannelWriter: dlState.writer, count: &dlState.publishCount},
		m.log,
		time.Duration(m.cfg.Storage.OffsetFlushIntervalMS)*time.Millisecond,
	)
	if err != nil {
		cancel()
		return fmt.Errorf("subscribe %q/%q: %w", channel, subscriberID, err)
	}
	sub.SetMaxAge(so.maxAge)
	sub.SetRetryBackoff(retryBase, retryMax)

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
			ID:            env.ID,
			Channel:       env.Channel,
			Origin:        env.Origin,
			PayloadType:   env.PayloadType,
			CorrelationID: env.CorrelationID,
			ServiceName:   env.ServiceName,
			Payload:       payload,
			Timestamp:     env.Ts,
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

// Stats returns a point-in-time snapshot of runtime metrics for this Messenger.
// Channel stream sizes are read from disk; subscriber positions are read from
// memory. The snapshot is not atomic across channels.
func (m *Messenger) Stats() Stats {
	var s Stats

	// Snapshot the channel map under read lock to avoid holding it during I/O.
	m.mu.RLock()
	channels := make(map[string]*channelState, len(m.channels))
	for k, v := range m.channels {
		channels[k] = v
	}
	m.mu.RUnlock()

	s.Channels = make([]ChannelStats, 0, len(channels))
	for name, cs := range channels {
		streamEnd, err := storage.ChannelStreamEnd(m.channelDir(name))
		if err != nil {
			m.log.Warn("stats: read channel stream end", "channel", name, "err", err)
		}
		diskBytes, err := storage.ChannelDiskBytes(m.channelDir(name))
		if err != nil {
			m.log.Warn("stats: read channel disk bytes", "channel", name, "err", err)
		}

		isDeadLetter := strings.HasSuffix(name, deadLetterSuffix)
		msgCount := cs.publishCount.Load()

		cs.mu.RLock()
		var channelLag int64
		subs := make([]SubscriberStats, 0, len(cs.subs))
		for id, entry := range cs.subs {
			off := entry.sub.Offset()
			lag := streamEnd - off
			if lag < 0 {
				lag = 0
			}
			channelLag += lag

			// Fold this subscriber's latency aggregates into the consume/handler
			// stages. Both stages sum across every subscriber, dead-letter ones
			// included — a dead-letter monitor is still a consumer.
			caSum, caCount := entry.sub.ConsumeAgeAggregate()
			s.Latency.Consume.SumNanos += caSum
			s.Latency.Consume.Count += caCount
			hlSum, hlCount := entry.sub.HandlerLatencyAggregate()
			s.Latency.Handler.SumNanos += hlSum
			s.Latency.Handler.Count += hlCount
			// When the subscriber is behind, read the timestamp of the record it is
			// parked on to expose time-based lag (byte lag alone hides a slow or
			// stalled consumer on a low-rate channel). Best-effort: log and skip on error.
			var oldestMs int64
			if off < streamEnd {
				if ts, ok, terr := storage.OldestPendingTimestamp(m.channelDir(name), off); terr != nil {
					m.log.Warn("stats: read oldest pending timestamp",
						"channel", name, "subscriber", id, "err", terr)
				} else if ok {
					oldestMs = ts.UnixMilli()
				}
			}
			subs = append(subs, SubscriberStats{ID: id, LagBytes: lag, OldestPendingUnixMs: oldestMs})
		}
		cs.mu.RUnlock()

		s.Channels = append(s.Channels, ChannelStats{
			Channel:      name,
			StreamBytes:  streamEnd,
			DiskBytes:    diskBytes,
			MessageCount: msgCount,
			Subscribers:  subs,
		})

		if isDeadLetter {
			s.Totals.DeadLetterMessages += msgCount
			s.Totals.DeadLetterBytes += diskBytes
		} else {
			s.Totals.Channels++
			s.Totals.Subscribers += len(subs)
			s.Totals.MessagesPublished += msgCount
			s.Totals.StreamBytes += streamEnd
			s.Totals.DiskBytes += diskBytes
			s.Totals.LagBytes += channelLag
		}
	}

	// Publish-to-disk latency is instance-wide, accumulated at the write sites
	// rather than derived from the per-channel snapshot.
	s.Latency.PublishToDisk.SumNanos = m.writeLatencySumNs.Load()
	s.Latency.PublishToDisk.Count = m.writeLatencyCount.Load()

	if len(m.clients) > 0 {
		s.Federation.Clients = make([]ClientStats, 0, len(m.clients))
		for _, c := range m.clients {
			rttSum, rttCount := c.AckRTT()
			s.Federation.Clients = append(s.Federation.Clients, ClientStats{
				HubAddr:        c.HubAddr(),
				Connected:      c.Connected(),
				ReconnectCount: c.ReconnectCount(),
				UnackedBytes:   c.UnackedBytes(),
				PublishRTT:     LatencyStage{Count: rttCount, SumNanos: rttSum},
			})
		}
	}

	if m.hub != nil {
		hs := m.hub.Stats()
		peers := make([]HubPeerStats, 0, len(hs.Peers))
		for _, p := range hs.Peers {
			peers = append(peers, HubPeerStats{
				Peer:        p.Peer,
				Addr:        p.Addr,
				Kind:        p.Kind,
				ConnectedAt: p.ConnectedAt,
				Channels:    p.Channels,
			})
		}
		s.Federation.Hub = &HubStats{
			Listening:           hs.Listening,
			Addr:                hs.Addr,
			PublishConns:        hs.PublishConns,
			SubscribeConns:      hs.SubscribeConns,
			RecordsReceived:     hs.RecordsReceived,
			BatchesReceived:     hs.BatchesReceived,
			ConnectionsAccepted: hs.ConnectionsAccepted,
			ConnectionsRejected: hs.ConnectionsRejected,
			SubscribeRTT:        LatencyStage{Count: hs.SubscribeRTTCount, SumNanos: hs.SubscribeRTTSumNanos},
			Peers:               peers,
		}
	}

	return s
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

// channelRetention returns the retention age the compactor should enforce for
// channel. Regular channels use the configured storage.retention (0 = keep
// until consumed). Dead-letter channels fall back to defaultDeadLetterRetention
// when no global retention is configured, so undrained dead letters cannot grow
// without bound; when a global retention IS configured, dead-letter channels
// inherit it like any other channel.
func (m *Messenger) channelRetention(channel string) time.Duration {
	retention := m.cfg.Storage.RetentionAge.Duration
	if retention == 0 && strings.HasSuffix(channel, deadLetterSuffix) {
		return defaultDeadLetterRetention
	}
	return retention
}

// channelRollThreshold returns the segment roll size in bytes for channel.
// Dead-letter channels use the smaller storage.dead_letter_compaction_threshold_mb
// so their active segment seals sooner and age-based retention can reclaim it;
// all other channels use storage.compaction_threshold_mb.
func (m *Messenger) channelRollThreshold(channel string) int64 {
	mb := m.cfg.Storage.CompactionThresholdMB
	if strings.HasSuffix(channel, deadLetterSuffix) {
		mb = m.cfg.Storage.DeadLetterCompactionThresholdMB
	}
	return int64(mb) * 1024 * 1024
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
			int64(m.cfg.Storage.MaxChannelSizeMB)*1024*1024,
			m.channelRetention(channel),
			m.log,
		),
	}

	// notifyFn is invoked by the writer goroutine after every successful write.
	// The closure captures cs by pointer so newly-added subscribers are notified.
	notifyFn := func() { cs.broadcastNotify() }

	var err error
	cs.writer, err = storage.NewChannelWriter(
		m.channelDir(channel),
		m.channelRollThreshold(channel),
		m.cfg.Storage.SyncIntervalMS,
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
// PeerReceivers after deduplication to store a remotely-published envelope,
// wake local subscribers, and (in a relay role) propagate the envelope on to
// outbound client peers. The LRU dedup catches any loop where a forwarded
// message returns to its origin.
func (m *Messenger) writeLocalEnvelope(env *envelope.Envelope) error {
	cs, err := m.getOrCreateChannelState(env.Channel)
	if err != nil {
		return err
	}
	writeStart := time.Now()
	if err := cs.writer.Write(context.Background(), env); err != nil {
		return err
	}
	m.recordWriteLatency(writeStart)
	cs.publishCount.Add(1)

	// Notify hub channel readers that new data is available for this channel.
	if m.hub != nil {
		m.hub.NotifyChannel(env.Channel)
	}
	// Also wake outbound client readers so a relay instance forwards the
	// message onward without waiting for a subsequent local publish.
	for _, c := range m.clients {
		if c.AllowPublish(env.Channel) {
			c.NotifyChannel(env.Channel)
		}
	}

	return nil
}

// writeLocalEnvelopeBatch is the federation localBatchWriter callback. It stores
// a batch of remotely-received envelopes durably as a unit (one fsync per
// channel group) before the receiver acks the batch, then wakes local
// subscribers and outbound client peers once per channel. Envelopes are grouped
// by channel because each WriteBatch targets one channel's segment; federation
// batches are single-channel today, but grouping keeps this correct regardless.
//
// A non-nil error means the batch was not fully committed; the receiver will not
// ack it, so the sender resends. The LRU dedup is updated by the receiver only
// after this returns nil, so a resend is not suppressed as a duplicate.
func (m *Messenger) writeLocalEnvelopeBatch(envs []*envelope.Envelope) error {
	for i := 0; i < len(envs); {
		channel := envs[i].Channel
		j := i + 1
		for j < len(envs) && envs[j].Channel == channel {
			j++
		}
		group := envs[i:j]

		cs, err := m.getOrCreateChannelState(channel)
		if err != nil {
			return err
		}
		writeStart := time.Now()
		if err := cs.writer.WriteBatch(context.Background(), group); err != nil {
			return err
		}
		m.recordWriteLatency(writeStart)
		cs.publishCount.Add(int64(len(group)))

		if m.hub != nil {
			m.hub.NotifyChannel(channel)
		}
		for _, c := range m.clients {
			if c.AllowPublish(channel) {
				c.NotifyChannel(channel)
			}
		}
		i = j
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
