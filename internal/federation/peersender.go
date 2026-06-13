package federation

import (
	"context"
	"errors"
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

// drainBufToUnacked moves any messages sitting in buf into the unacked list so
// they are not silently dropped when the run goroutine exits due to a stream
// error. The caller must NOT hold ps.mu.
func (ps *PeerSender) drainBufToUnacked() {
	ps.mu.Lock()
	for {
		select {
		case env := <-ps.buf:
			ps.unacked = append(ps.unacked, env)
		default:
			ps.mu.Unlock()
			return
		}
	}
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
			// Stream closed by the remote before the next message was sent.
			// Drain any pending messages into unacked so the reconnect loop
			// can replay them rather than losing them.
			ps.drainBufToUnacked()
			return
		case first = <-ps.buf:
		}

		// Accumulate a batch up to maxBatchBytes or until the buffer is empty.
		// Marshal each envelope once: the bytes serve both the size check and Send.
		firstData, mErr := envelope.Marshal(*first)
		if mErr != nil {
			ps.log.Error("federation: sender marshal", "id", first.ID, "err", mErr)
			continue
		}
		envs := []*envelope.Envelope{first}
		records := [][]byte{firstData}
		batchBytes := len(firstData)

	drain:
		for batchBytes < ps.maxBatchBytes {
			select {
			case env := <-ps.buf:
				data, mErr := envelope.Marshal(*env)
				if mErr != nil {
					ps.log.Error("federation: sender marshal", "id", env.ID, "err", mErr)
					continue drain
				}
				envs = append(envs, env)
				records = append(records, data)
				batchBytes += len(data)
			default:
				break drain
			}
		}

		// Send one batch containing all records.
		if err := ps.stream.Send(&federationv1.PublishBatch{Records: records}); err != nil {
			ps.log.Error("federation: sender send batch", "err", err)
			// These messages were collected but never delivered; add them to
			// unacked so the reconnect loop can replay them.
			ps.mu.Lock()
			ps.unacked = append(ps.unacked, envs...)
			ps.mu.Unlock()
			ps.drainBufToUnacked()
			return
		}

		// Record as unacked.
		ps.mu.Lock()
		ps.unacked = append(ps.unacked, envs...)
		ps.mu.Unlock()

		// Block until ack arrives from the remote receiver.
		ack, err := ps.stream.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) && status.Code(err) != codes.Canceled {
				ps.log.Error("federation: sender receive ack", "err", err)
			}
			ps.drainBufToUnacked()
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
