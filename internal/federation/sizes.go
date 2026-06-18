package federation

// Federation message-size limits.
//
// maxRecordBytes is the largest single envelope record the federation layer
// will deliver. A record larger than this can never fit inside the gRPC frame,
// so readBatch drops it (advancing past it) instead of retrying it forever —
// otherwise one oversized message wedges a channel in an endless
// disconnect/redeliver loop.
//
// The gRPC frame limit sits above maxRecordBytes (and above any configured
// maxBatchBytes) to leave room for EnvelopeBatch/PublishBatch protobuf framing
// around the records it carries.
// These are var rather than const only so tests can shrink them to exercise the
// oversized-record and gRPC frame-limit paths with kilobyte payloads instead of
// allocating multi-megabyte records. Production code never reassigns them.
var (
	// maxRecordBytes is the per-record deliverability cap (10 MiB).
	maxRecordBytes = 10 * 1024 * 1024

	// grpcFramingHeadroom is the slack added on top of the largest payload a
	// frame may carry, to cover protobuf field tags and length prefixes.
	grpcFramingHeadroom = 1 * 1024 * 1024
)

// grpcMessageLimit returns the gRPC send/recv message-size limit for a peer
// configured with the given maxBatchBytes. A frame must accommodate the larger
// of a full batch or a single max-sized record, plus framing overhead.
func grpcMessageLimit(maxBatchBytes int) int {
	biggest := maxRecordBytes
	if maxBatchBytes > biggest {
		biggest = maxBatchBytes
	}
	return biggest + grpcFramingHeadroom
}
