package federation

import (
	"encoding/json"
	"io"

	"github.com/gorilla/websocket"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/envelope"
)

// Deduplicator is satisfied by dedup.LRUDedup. Defined locally to avoid an
// import cycle between federation and dedup.
type Deduplicator interface {
	SeenOrAdd(id string) bool
}

// PeerReceiver reads binary data frames from conn, deduplicates, enforces the
// receive policy, and hands each accepted envelope to localWriter. After each
// batch it sends a text-frame ack. Blocking in localWriter propagates TCP
// backpressure to the sender — this is intentional.
//
// When ackRouteCh is non-nil this receiver owns ALL reads on the connection;
// text-frame acks destined for a co-located PeerSender are forwarded to
// ackRouteCh. ackRouteCh is closed when the receiver exits.
type PeerReceiver struct {
	conn          *websocket.Conn
	policy        *AtomicPolicy // may be nil (accept everything)
	dedup         Deduplicator
	localWriter   func(*envelope.Envelope) error
	auditL        audit.AuditLogger
	log           logger
	peerName      string
	maxBatchBytes int
	ackRouteCh    chan<- AckMsg // non-nil when sharing conn with a PeerSender

	stop chan struct{}
	done chan struct{}
}

// NewPeerReceiver creates a PeerReceiver and starts its goroutine.
func NewPeerReceiver(
	conn *websocket.Conn,
	policy *AtomicPolicy,
	dedup Deduplicator,
	localWriter func(*envelope.Envelope) error,
	auditL audit.AuditLogger,
	log logger,
	peerName string,
	maxBatchBytes int,
) *PeerReceiver {
	pr := &PeerReceiver{
		conn:          conn,
		policy:        policy,
		dedup:         dedup,
		localWriter:   localWriter,
		auditL:        auditL,
		log:           log,
		peerName:      peerName,
		maxBatchBytes: maxBatchBytes,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	go pr.run()
	return pr
}

// newPeerReceiverWithAck creates a PeerReceiver that owns all reads on conn and
// routes text-frame acks to ackRouteCh. Use this when a PeerSender also uses
// the same conn to avoid concurrent reads. ackRouteCh is closed when the
// receiver goroutine exits, which will unblock the paired PeerSender.
func newPeerReceiverWithAck(
	conn *websocket.Conn,
	policy *AtomicPolicy,
	dedup Deduplicator,
	localWriter func(*envelope.Envelope) error,
	auditL audit.AuditLogger,
	log logger,
	peerName string,
	maxBatchBytes int,
	ackRouteCh chan<- AckMsg,
) *PeerReceiver {
	pr := &PeerReceiver{
		conn:          conn,
		policy:        policy,
		dedup:         dedup,
		localWriter:   localWriter,
		auditL:        auditL,
		log:           log,
		peerName:      peerName,
		maxBatchBytes: maxBatchBytes,
		ackRouteCh:    ackRouteCh,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	go pr.run()
	return pr
}

// Done returns a channel closed when the receiver goroutine exits.
func (pr *PeerReceiver) Done() <-chan struct{} { return pr.done }

// Close signals the receiver to stop and waits for exit.
func (pr *PeerReceiver) Close() {
	select {
	case <-pr.stop:
	default:
		close(pr.stop)
	}
	_ = pr.conn.Close()
	<-pr.done
}

func (pr *PeerReceiver) run() {
	defer close(pr.done)
	if pr.ackRouteCh != nil {
		defer close(pr.ackRouteCh)
	}

	for {
		msgType, r, err := pr.conn.NextReader()
		if err != nil {
			select {
			case <-pr.stop:
			default:
				pr.log.Error("federation: receiver read error", "peer", pr.peerName, "err", err)
			}
			return
		}
		if msgType != websocket.BinaryMessage {
			// When sharing a conn with a PeerSender, text frames are acks
			// destined for that sender — route them via ackRouteCh.
			if pr.ackRouteCh != nil && msgType == websocket.TextMessage {
				data, readErr := io.ReadAll(r)
				if readErr == nil {
					var ack AckMsg
					if jsonErr := json.Unmarshal(data, &ack); jsonErr == nil {
						select {
						case pr.ackRouteCh <- ack:
						case <-pr.stop:
							return
						}
					}
				}
			} else {
				pr.log.Warn("federation: receiver unexpected frame type", "type", msgType)
			}
			continue
		}

		records, err := ReadFrame(r, pr.maxBatchBytes)
		if err != nil {
			pr.log.Error("federation: receiver read frame", "peer", pr.peerName, "err", err)
			return
		}

		var lastID string
		for _, rec := range records {
			env, err := envelope.Unmarshal(rec)
			if err != nil {
				pr.log.Error("federation: receiver unmarshal", "err", err)
				continue
			}

			if pr.dedup.SeenOrAdd(env.ID) {
				continue // already processed
			}

			// Receive policy check — discard silently but still ack.
			if pr.policy != nil && !pr.policy.AllowReceive(env.Channel) {
				pr.log.Warn("federation: receive policy violation",
					"channel", env.Channel, "peer", pr.peerName)
				_ = pr.auditL.Log(audit.Event{
					Event:     audit.EventPolicyViolation,
					Channel:   env.Channel,
					Peer:      pr.peerName,
					Direction: "inbound",
				})
				lastID = env.ID
				continue
			}

			// Write to local storage — blocks on disk full (backpressure).
			envCopy := env
			if err := pr.localWriter(&envCopy); err != nil {
				pr.log.Error("federation: receiver local write", "id", env.ID, "err", err)
				lastID = env.ID
				continue
			}

			_ = pr.auditL.Log(audit.Event{
				Event:     audit.EventForward,
				MessageID: env.ID,
				Channel:   env.Channel,
				Peer:      pr.peerName,
				Direction: "inbound",
			})
			lastID = env.ID
		}

		// Ack the full batch — even policy-violated records are acked so the
		// sender can advance its window.
		if err := SendAck(pr.conn, AckMsg{LastID: lastID}); err != nil {
			pr.log.Error("federation: receiver send ack", "err", err)
			return
		}
	}
}
