package federation

import (
	"io"
	"sync"

	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/envelope"
)

// Deduplicator is satisfied by dedup.LRUDedup. Defined locally to avoid an
// import cycle between federation and dedup.
type Deduplicator interface {
	SeenOrAdd(id string) bool
}

// PeerReceiver reads EnvelopeBatch messages from the Subscribe gRPC stream,
// deduplicates, enforces the receive policy, and hands each accepted envelope
// to localWriter. After each batch it sends a SubscribeFrame Ack back to the
// hub. Blocking in localWriter propagates backpressure to the sender — intentional.
//
// PeerReceiver owns all reads and writes on its Subscribe stream; no mutex needed.
type PeerReceiver struct {
	stream        federationv1.FederationService_SubscribeClient
	streamCancel  func() // cancels the stream context on Close
	policy        *AtomicPolicy
	dedup         Deduplicator
	localWriter   func(*envelope.Envelope) error
	auditL        audit.AuditLogger
	log           logger
	peerName      string
	maxBatchBytes int

	mu             sync.Mutex
	lastReceivedID string // safe to read after Done()
	stopErr        error  // non-nil error that caused the receiver to exit

	stop chan struct{}
	done chan struct{}
}

// LastReceivedID returns the envelope ID of the last batch this receiver
// processed and sent an ack for. Intended to be read after Done() fires.
func (pr *PeerReceiver) LastReceivedID() string {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return pr.lastReceivedID
}

// NewPeerReceiver creates a PeerReceiver and starts its goroutine.
func NewPeerReceiver(
	stream federationv1.FederationService_SubscribeClient,
	streamCancel func(),
	policy *AtomicPolicy,
	dedup Deduplicator,
	localWriter func(*envelope.Envelope) error,
	auditL audit.AuditLogger,
	log logger,
	peerName string,
	maxBatchBytes int,
) *PeerReceiver {
	pr := &PeerReceiver{
		stream:        stream,
		streamCancel:  streamCancel,
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

// Done returns a channel closed when the receiver goroutine exits.
func (pr *PeerReceiver) Done() <-chan struct{} { return pr.done }

// Err returns the error that caused the receiver to exit, or nil for a clean
// shutdown. Safe to call only after Done() has been closed.
func (pr *PeerReceiver) Err() error {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return pr.stopErr
}

// Close signals the receiver to stop and waits for exit.
func (pr *PeerReceiver) Close() {
	select {
	case <-pr.stop:
	default:
		close(pr.stop)
	}
	pr.streamCancel() // cancel stream context to unblock Recv
	<-pr.done
}

func (pr *PeerReceiver) run() {
	defer close(pr.done)

	for {
		msg, err := pr.stream.Recv()
		if err != nil {
			select {
			case <-pr.stop:
			default:
				if err != io.EOF {
					pr.log.Error("federation: receiver read error", "peer", pr.peerName, "err", err)
					pr.mu.Lock()
					pr.stopErr = err
					pr.mu.Unlock()
				}
			}
			return
		}

		switch p := msg.Payload.(type) {
		case *federationv1.HubBatch_Close:
			pr.log.Info("federation: receiver got close notice", "peer", pr.peerName,
				"code", p.Close.Code, "text", p.Close.Text)
			return
		case *federationv1.HubBatch_Batch:
			if err := pr.processBatch(p.Batch); err != nil {
				return
			}
		default:
			pr.log.Warn("federation: receiver unexpected hub batch payload type")
		}
	}
}

func (pr *PeerReceiver) processBatch(batch *federationv1.EnvelopeBatch) error {
	var lastID string

	for _, rec := range batch.Records {
		if pr.maxBatchBytes > 0 && len(rec) > pr.maxBatchBytes {
			if env, uErr := envelope.Unmarshal(rec); uErr == nil {
				pr.log.Error("federation: receiver record too large, skipping",
					"peer", pr.peerName, "id", env.ID, "channel", env.Channel,
					"size", len(rec), "max", pr.maxBatchBytes)
				lastID = env.ID
			}
			continue
		}

		env, err := envelope.Unmarshal(rec)
		if err != nil {
			pr.log.Error("federation: receiver unmarshal", "err", err)
			continue
		}

		if pr.dedup.SeenOrAdd(env.ID) {
			continue
		}

		if pr.policy != nil && !pr.policy.AllowReceive(env.Channel) {
			pr.log.Warn("federation: receive policy violation",
				"channel", env.Channel, "peer", pr.peerName, "id", env.ID)
			_ = pr.auditL.Log(audit.Event{
				Event:     audit.EventPolicyViolation,
				Channel:   env.Channel,
				Peer:      pr.peerName,
				Direction: "inbound",
				MessageID: env.ID,
			})
			lastID = env.ID
			continue
		}

		envCopy := env
		if err := pr.localWriter(&envCopy); err != nil {
			pr.log.Error("federation: receiver local write", "id", env.ID, "err", err, "channel", env.Channel)
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

	if lastID != "" {
		pr.mu.Lock()
		pr.lastReceivedID = lastID
		pr.mu.Unlock()
	}

	// Ack the full batch. PeerReceiver is the sole writer on the Subscribe stream
	// client-side, so no mutex is needed.
	if ackErr := pr.stream.Send(&federationv1.SubscribeFrame{
		Payload: &federationv1.SubscribeFrame_Ack{
			Ack: &federationv1.Ack{LastId: lastID},
		},
	}); ackErr != nil {
		pr.log.Error("federation: receiver send ack", "err", ackErr)
		return ackErr
	}
	return nil
}
