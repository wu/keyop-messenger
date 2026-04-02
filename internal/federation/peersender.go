package federation

import (
	"sync"

	"github.com/gorilla/websocket"
	"github.com/keyop/keyop-messenger/internal/envelope"
)

// PeerSender manages outbound delivery to one peer over a WebSocket connection.
// One goroutine owns all writes; it batches, sends a binary frame, then blocks
// waiting for a text-frame ack before sending the next batch. Unacked messages
// are retained for replay on reconnect.
type PeerSender struct {
	conn          *websocket.Conn
	maxBatchBytes int
	log           logger

	buf  chan *envelope.Envelope // bounded outbound buffer
	stop chan struct{}
	done chan struct{} // closed when the run goroutine exits

	mu          sync.Mutex
	unacked     []*envelope.Envelope // sent but not yet acked
	lastAckedID string
}

// NewPeerSender starts a PeerSender goroutine on conn.
// bufSize is the capacity of the outbound buffer (send_buffer_messages).
func NewPeerSender(conn *websocket.Conn, bufSize, maxBatchBytes int, log logger) *PeerSender {
	ps := &PeerSender{
		conn:          conn,
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
		ps.log.Warn("federation: peer send buffer full, dropping message", "id", env.ID)
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
	_ = ps.conn.Close() // unblock ReceiveAck if blocked in run()
	<-ps.done
}

func (ps *PeerSender) run() {
	defer close(ps.done)

	for {
		// Wait for at least one message or stop signal.
		var first *envelope.Envelope
		select {
		case <-ps.stop:
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

		// Write one binary frame containing all records.
		w, err := ps.conn.NextWriter(websocket.BinaryMessage)
		if err != nil {
			ps.log.Error("federation: sender next writer", "err", err)
			return
		}
		if err := WriteFrame(w, records); err != nil {
			_ = w.Close()
			ps.log.Error("federation: sender write frame", "err", err)
			return
		}
		if err := w.Close(); err != nil {
			ps.log.Error("federation: sender close writer", "err", err)
			return
		}

		// Record as unacked.
		ps.mu.Lock()
		ps.unacked = append(ps.unacked, batch...)
		ps.mu.Unlock()

		// Block until ack arrives from the remote receiver.
		ack, err := ReceiveAck(ps.conn)
		if err != nil {
			ps.log.Error("federation: sender receive ack", "err", err)
			return
		}

		// Advance the unacked window up to ack.LastID.
		if ack.LastID != "" {
			ps.mu.Lock()
			ps.lastAckedID = ack.LastID
			for i, env := range ps.unacked {
				if env.ID == ack.LastID {
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
