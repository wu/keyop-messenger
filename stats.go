package messenger

import "time"

// Stats is a point-in-time snapshot of runtime metrics for a Messenger
// instance. Values are best-effort reads; use Messenger.Stats() to obtain a fresh snapshot.
// The snapshot is not atomic across channels.
type Stats struct {
	// Channels contains one entry per known channel, including dead-letter channels.
	// Order is not guaranteed.
	Channels []ChannelStats
	// Federation contains stats for each configured outbound hub connection.
	Federation FederationStats
}

// ChannelStats holds metrics for one channel.
type ChannelStats struct {
	// Channel is the channel name.
	Channel string
	// StreamBytes is the byte position of the end of the channel's write stream —
	// the total bytes ever appended, which is monotonically increasing. This value
	// does not decrease when compaction removes consumed segments.
	StreamBytes int64
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
}
