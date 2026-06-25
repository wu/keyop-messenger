package messenger

import "time"

// Stats is a point-in-time snapshot of runtime metrics for a Messenger
// instance. Values are best-effort reads; use Messenger.Stats() to obtain a fresh snapshot.
// The snapshot is not atomic across channels.
type Stats struct {
	// Channels contains one entry per known channel, including dead-letter channels.
	// Order is not guaranteed.
	Channels []ChannelStats
	// Totals holds instance-wide aggregates pre-summed from Channels, so metrics
	// exporters (Graphite, etc.) don't have to walk per-channel/subscriber data.
	Totals Totals
	// Latency holds per-stage latency aggregates — the fourth golden signal.
	Latency Latency
	// Federation contains stats for each configured outbound hub connection.
	Federation FederationStats
}

// Latency holds running latency aggregates for each pipeline stage. Latency is
// a distribution, not a sum, so rather than a single number each stage exposes a
// sample count and the summed duration; an exporter derives the average over an
// interval as Δ(SumNanos)/Δ(Count) — matching the count/sum style of Totals. All
// values are start-relative counters that reset to zero on process restart, so
// graph the deltas (e.g. Graphite nonNegativeDerivative), not the absolute level.
type Latency struct {
	// PublishToDisk measures the time from a Publish/PublishBatch (or a
	// federation-received write) to the message being durably written (fsync),
	// on the writing host's own clock. One sample per write operation: a batch
	// counts once, with the whole batch's commit time.
	PublishToDisk LatencyStage
	// Consume measures the end-to-end delivery age of successfully consumed
	// messages: the time from publish (the envelope timestamp) to a subscriber's
	// handler returning success. It spans the publisher and consumer clocks, so
	// across federated hosts it carries clock skew (clamped to >= 0).
	Consume LatencyStage
	// Handler measures the time spent inside subscriber handlers for successfully
	// consumed messages, on the consumer's own clock. Unlike Consume it excludes
	// queue wait, isolating handler cost from delivery delay.
	Handler LatencyStage
}

// LatencyStage is one stage's running aggregate: the number of samples observed
// and their summed duration in nanoseconds. The mean is SumNanos/Count (guard
// against Count == 0).
type LatencyStage struct {
	// Count is the number of samples observed since process start.
	Count int64
	// SumNanos is the summed duration of all samples, in nanoseconds.
	SumNanos int64
}

// Totals holds instance-wide aggregates derived from the per-channel stats in a
// Stats snapshot. They are pre-summed so callers shipping metrics to systems
// like Graphite don't need to re-sum the per-channel and per-subscriber data.
// Dead-letter channels are excluded from the non-dead-letter totals and reported
// separately as the error signal. All values are point-in-time, matching the
// Channels slice they were computed from.
type Totals struct {
	// Channels is the count of non-dead-letter channels. (gauge)
	Channels int
	// Subscribers is the count of active subscribers across all non-dead-letter
	// channels. (gauge)
	Subscribers int
	// MessagesPublished is the sum of MessageCount over non-dead-letter channels.
	// Like the per-channel counter, it resets on process restart. (traffic)
	MessagesPublished int64
	// StreamBytes is the sum of StreamBytes over non-dead-letter channels — a
	// monotonic byte-throughput counter that, unlike DiskBytes, does not drop
	// when compaction removes consumed segments. (traffic)
	StreamBytes int64
	// DiskBytes is the sum of DiskBytes over non-dead-letter channels: the
	// instance's current on-disk footprint for live message data. (saturation)
	DiskBytes int64
	// LagBytes is the sum of LagBytes over every subscriber on non-dead-letter
	// channels — the total unread backlog. This is the primary saturation signal:
	// a rising value means consumers are falling behind. (saturation)
	LagBytes int64
	// DeadLetterMessages is the sum of MessageCount over dead-letter channels.
	// Like other message counters it resets on process restart. (errors)
	DeadLetterMessages int64
	// DeadLetterBytes is the sum of DiskBytes over dead-letter channels: the
	// current on-disk footprint of dead-lettered messages. (errors)
	DeadLetterBytes int64
}

// ChannelStats holds metrics for one channel.
type ChannelStats struct {
	// Channel is the channel name.
	Channel string
	// StreamBytes is the byte position of the end of the channel's write stream —
	// the total bytes ever appended, which is monotonically increasing. This value
	// does not decrease when compaction removes consumed segments.
	StreamBytes int64
	// DiskBytes is the channel's current on-disk footprint: the sum of the sizes
	// of its surviving segment files. Unlike StreamBytes, this shrinks when
	// compaction removes consumed segments, so it reflects actual disk usage.
	DiskBytes int64
	// MessageCount is the number of messages published to this channel since the
	// Messenger was started. This counter resets on process restart.
	MessageCount int64
	// Subscribers contains one entry per currently active subscriber.
	Subscribers []SubscriberStats
}

// SubscriberStats holds metrics for one subscriber on a channel.
type SubscriberStats struct {
	// ID is the subscriber's registered identifier.
	ID string
	// LagBytes is the number of unread bytes between this subscriber's current
	// position and the end of the channel stream.
	LagBytes int64
	// OldestPendingUnixMs is the publish timestamp (Unix milliseconds, UTC) of the
	// oldest message this subscriber has not yet consumed — the record sitting at
	// its current offset. It is 0 when the subscriber is caught up, or when the
	// timestamp could not be read. Callers compute the actual age as now minus this
	// value; doing the subtraction at display time keeps it correct between polls
	// and avoids baking hub/consumer clock skew into a server-side number.
	OldestPendingUnixMs int64
}

// FederationStats holds metrics for federation: outbound hub connections and,
// when this instance runs a hub, inbound hub-side metrics.
type FederationStats struct {
	// Clients contains one entry per configured hub connection.
	Clients []ClientStats
	// Hub holds inbound, hub-side metrics. It is nil when this instance is not
	// running a hub listener.
	Hub *HubStats
}

// HubStats is a point-in-time snapshot of inbound metrics for a hub listener.
type HubStats struct {
	// Listening reports whether the hub has an active listener.
	Listening bool
	// Addr is the hub's listen address, or empty when not listening.
	Addr string
	// PublishConns is the number of currently active inbound Publish streams.
	PublishConns int
	// SubscribeConns is the number of currently active inbound Subscribe streams.
	SubscribeConns int
	// RecordsReceived is the total number of envelope records committed via the
	// Publish RPC since the hub started.
	RecordsReceived int64
	// BatchesReceived is the total number of Publish batches committed since the
	// hub started.
	BatchesReceived int64
	// ConnectionsAccepted is the cumulative number of peer streams that passed
	// authentication and the allowlist check.
	ConnectionsAccepted int64
	// ConnectionsRejected is the cumulative number of peer streams refused by the
	// allowlist.
	ConnectionsRejected int64
	// SubscribeRTT is the hub→peer delivery latency aggregate: the running count
	// and summed duration of Subscribe-stream batch send→ack round-trips, measured
	// hub-wide on the hub's own clock (free of cross-host skew). It is the inbound
	// counterpart to ClientStats.PublishRTT. Like the Latency stages, the mean is
	// SumNanos/Count and the values reset on restart.
	SubscribeRTT LatencyStage
	// SubscribeSendFailures is the number of hub→peer batches that failed to be
	// acked because the Subscribe stream broke (excluding deliberate shutdown),
	// hub-wide since the hub started. It is the error counterpart to SubscribeRTT.
	SubscribeSendFailures int64
	// Peers contains one entry per currently connected inbound peer stream.
	Peers []HubPeerStats
}

// HubPeerStats describes one currently-connected inbound peer stream.
type HubPeerStats struct {
	// Peer is the authenticated peer identity (TLS cert CN).
	Peer string
	// Addr is the peer's remote network address.
	Addr string
	// Kind is "publish" or "subscribe".
	Kind string
	// ConnectedAt is when the stream was accepted.
	ConnectedAt time.Time
	// Channels lists the channels this stream publishes or subscribes to.
	Channels []string
}

// ClientStats holds metrics for one outbound hub connection.
type ClientStats struct {
	// HubAddr is the network address of the hub.
	HubAddr string
	// Connected reports whether the gRPC Publish stream is currently active.
	Connected bool
	// ReconnectCount is the number of successful reconnections after the initial
	// dial. The first connection is not counted.
	ReconnectCount int64
	// UnackedBytes is the total bytes of locally-published messages on this
	// client's outbound channels that have not yet been acknowledged by the hub.
	// Computed from the per-channel offset files against the channel stream end.
	UnackedBytes int64
	// PublishRTT is the client→hub transit latency aggregate: the running count
	// and summed duration of PublishBatch send→ack round-trips, measured on this
	// client's own clock (free of cross-host clock skew). Like the Latency
	// stages, the mean is SumNanos/Count and the values reset on restart.
	PublishRTT LatencyStage
	// PublishSendFailures is the number of outbound batches that failed to be
	// acked because the stream broke (excluding deliberate shutdown), since this
	// client started. It is the error counterpart to PublishRTT: PublishRTT
	// measures successful deliveries, this counts disrupted ones. A rising value
	// indicates trouble reaching the hub.
	PublishSendFailures int64
}
