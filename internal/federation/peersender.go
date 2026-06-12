package federation

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/envelope"
)

// PeerSender manages outbound delivery to one peer over the Publish gRPC stream.
// One goroutine owns all writes; it batches, sends a PublishBatch, then blocks
// waiting for a PublishAck before sending the next batch. Unacked messages
// are retained for replay on reconnect.
type PeerSender struct {
	stream        federationv1.FederationService_PublishClient
	streamCancel  context.CancelFunc
	maxBatchBytes int
	log           logger

	buf  chan *envelope.Envelope // bounded outbound buffer
	stop chan struct{}
	done chan struct{} // closed when the run goroutine exits

	mu          sync.Mutex
	unacked     []*envelope.Envelope // sent but not yet acked
	lastAckedID string
}

// NewPeerSender starts a PeerSender goroutine on the given Publish stream.
// streamCancel cancels the stream context; call it in Close() to unblock Recv.
// bufSize is the capacity of the outbound buffer.
func NewPeerSender(
	stream federationv1.FederationService_PublishClient,
	streamCancel context.CancelFunc,
	bufSize, maxBatchBytes int,
	log logger,
) *PeerSender {
	ps := &PeerSender{
		stream:        stream,
		streamCancel:  streamCancel,
		maxBatchBytes: maxBatchBytes,
		log:           log,
		buf:           make(chan *envelope.Envelope, bufSize),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	go ps.run()
	return ps
}

// Enqueue adds env to the outbound buffer. Returns false (and logs a warning)
// if the buffer is full — the message is dropped, not blocked.
func (ps *PeerSender) Enqueue(env *envelope.Envelope) bool {
	select {
	case ps.buf <- env:
		return true
	default:
		ps.log.Warn("federation: peer send buffer full, dropping message", "id", env.ID, "channel", env.Channel)
		return false
	}
}

// LastAckedID returns the ID of the last positively acknowledged message.
func (ps *PeerSender) LastAckedID() string {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.lastAckedID
}

// Unacked returns a snapshot of messages sent but not yet acked. Safe to call
// after Close(); used by the Client to replay on reconnect.
func (ps *PeerSender) Unacked() []*envelope.Envelope {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	out := make([]*envelope.Envelope, len(ps.unacked))
	copy(out, ps.unacked)
	return out
}

// Done returns a channel closed when the sender goroutine exits. A close due
// to a write error fires Done without Close being called first; the caller
// should use this to detect connection loss.
func (ps *PeerSender) Done() <-chan struct{} { return ps.done }

// Close signals the sender to stop and waits for exit.
func (ps *PeerSender) Close() {
	select {
	case <-ps.stop:
	default:
		close(ps.stop)
	}
	ps.streamCancel() // cancel stream context to unblock Recv
	<-ps.done
}

func (ps *PeerSender) run() {
	defer close(ps.done)

	for {
		// Wait for at least one message, stop signal, or stream context cancellation.
		var first *envelope.Envelope
		select {
		case <-ps.stop:
			return
		case <-ps.stream.Context().Done():
			return
		case first = <-ps.buf:
		}

		// Accumulate a batch up to maxBatchBytes or until the buffer is empty.
		batch := []*envelope.Envelope{first}
		batchBytes := marshaledSize(first)

	drain:
		for batchBytes < ps.maxBatchBytes {
			select {
			case env := <-ps.buf:
				batch = append(batch, env)
				batchBytes += marshaledSize(env)
			default:
				break drain
			}
		}

		// Marshal each envelope into a wire record.
		records := make([][]byte, 0, len(batch))
		for _, env := range batch {
			data, err := envelope.Marshal(*env)
			if err != nil {
				ps.log.Error("federation: sender marshal", "id", env.ID, "err", err)
				continue
			}
			records = append(records, data)
		}
		if len(records) == 0 {
			continue
		}

		// Send one batch containing all records.
		if err := ps.stream.Send(&federationv1.PublishBatch{Records: records}); err != nil {
			ps.log.Error("federation: sender send batch", "err", err)
			return
		}

		// Record as unacked.
		ps.mu.Lock()
		ps.unacked = append(ps.unacked, batch...)
		ps.mu.Unlock()

		// Block until ack arrives from the remote receiver.
		ack, err := ps.stream.Recv()
		if err != nil {
			if err != io.EOF && status.Code(err) != codes.Canceled {
				ps.log.Error("federation: sender receive ack", "err", err)
			}
			return
		}

		// Advance the unacked window up to ack.LastId.
		if ack.LastId != "" {
			ps.mu.Lock()
			ps.lastAckedID = ack.LastId
			for i, env := range ps.unacked {
				if env.ID == ack.LastId {
					ps.unacked = ps.unacked[i+1:]
					break
				}
			}
			ps.mu.Unlock()
		}
	}
}

// marshaledSize returns an estimate of the wire size of env for batch sizing.
// On marshal error it returns 0 (the envelope is skipped later).
func marshaledSize(env *envelope.Envelope) int {
	data, err := envelope.Marshal(*env)
	if err != nil {
		return 0
	}
	return len(data)
}
