package messenger

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

// FederationStats holds metrics for outbound hub connections.
type FederationStats struct {
	// Clients contains one entry per configured hub connection.
	Clients []ClientStats
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
