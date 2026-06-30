package federation

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/audit"
)

// pubCoordinator serialises outbound delivery from all of one client's
// channelReaders onto a single client-side Publish gRPC stream. One instance
// is created per active connection to a hub.
//
// A send goroutine dequeues sendReqs from requestCh (shared by all readers for
// this client), sends a PublishBatch, then waits for a PublishAck from the
// ack-reader goroutine before unblocking the originating channelReader by
// closing req.doneCh. The single send goroutine guarantees ordered delivery.
//
// On stream error the coordinator exits; the Client's reconnect loop closes
// the readers, rebuilds them on the new connection, and resumes from the
// persisted offset files. There is no in-memory unacked window — the offset
// file IS the delivery state.
type pubCoordinator struct {
	stream       federationv1.FederationService_PublishClient
	streamCancel context.CancelFunc
	log          logger
	readers      []*channelReader
	requestCh    chan sendReq
	// recordAckRTT reports each batch's send→ack round-trip time to the owning
	// Client, which accumulates it across reconnects.
	recordAckRTT func(time.Duration)
	// recordSendFail reports an in-flight batch that failed to be acked because
	// the stream broke (not a deliberate shutdown), so the Client can surface a
	// delivery-failure count.
	recordSendFail func()
	// auditL records an outbound forward event per envelope once the hub acks a
	// batch. peer identifies the destination hub in those records.
	auditL audit.AuditLogger
	peer   string

	ackCh   chan struct{}
	ackDone chan struct{}

	stop chan struct{}
	done chan struct{}

	closeOnce sync.Once
}

// newPubCoordinator constructs a pubCoordinator and wires its requestCh into
// each provided channelReader. start() must be called to launch the goroutines.
//
// streamCancel must cancel the context that stream is using so that Close
// can unblock the ack-reader's blocking Recv.
func newPubCoordinator(
	stream federationv1.FederationService_PublishClient,
	streamCancel context.CancelFunc,
	log logger,
	readers []*channelReader,
	recordAckRTT func(time.Duration),
	recordSendFail func(),
	auditL audit.AuditLogger,
	peer string,
) *pubCoordinator {
	requestCh := make(chan sendReq, 1)
	pc := &pubCoordinator{
		stream:         stream,
		streamCancel:   streamCancel,
		log:            log,
		readers:        readers,
		recordAckRTT:   recordAckRTT,
		recordSendFail: recordSendFail,
		auditL:         auditL,
		peer:           peer,
		requestCh:      requestCh,
		ackCh:          make(chan struct{}, 1),
		ackDone:        make(chan struct{}),
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
	}
	for _, r := range readers {
		r.requestCh = requestCh
	}
	return pc
}

// start launches the ack-reader, send goroutine, and all channel readers.
func (pc *pubCoordinator) start() {
	go pc.ackReader()
	for _, r := range pc.readers {
		r.start()
	}
	go pc.run()
}

// close stops the send goroutine and all channel readers, cancels the stream
// context so the ack-reader's blocking Recv unblocks, and waits for everything
// to exit. Safe to call multiple times and concurrently — the Client's reconnect
// loop and Client.Close can both reach this path when a disconnect and a shutdown
// race. The teardown runs exactly once; concurrent callers block until it finishes.
func (pc *pubCoordinator) close() {
	pc.closeOnce.Do(func() {
		close(pc.stop)
		<-pc.done
		// Cancel the stream context so ackReader's blocking Recv returns.
		pc.streamCancel()
		<-pc.ackDone
		for _, r := range pc.readers {
			r.close()
		}
	})
}

// Done returns a channel that is closed when the send goroutine exits, either
// because of a stream error or because Close() was called. The Client's
// reconnect loop selects on this.
func (pc *pubCoordinator) Done() <-chan struct{} { return pc.done }

// run dequeues sendReqs from channelReaders and dispatches them sequentially.
func (pc *pubCoordinator) run() {
	defer close(pc.done)
	streamDone := pc.stream.Context().Done()
	for {
		select {
		case <-pc.stop:
			return
		case <-streamDone:
			return
		case req := <-pc.requestCh:
			if err := pc.sendBatch(req); err != nil {
				if !errors.Is(err, io.EOF) && status.Code(err) != codes.Canceled {
					pc.log.Error("pubCoordinator: send batch failed",
						"channel", req.channel, "err", err)
				}
				return
			}
		}
	}
}

// sendBatch sends one PublishBatch and waits for the corresponding PublishAck.
// Closes req.doneCh on successful ack; returns the error otherwise. The
// channelReader sleeps on its sendReq.doneCh; on success it persists the new
// offset and reads the next batch.
func (pc *pubCoordinator) sendBatch(req sendReq) error {
	sendStart := time.Now()
	if err := pc.stream.Send(&federationv1.PublishBatch{Records: req.rawLines}); err != nil {
		pc.sendFailed()
		return err
	}

	select {
	case _, ok := <-pc.ackCh:
		if !ok {
			pc.sendFailed()
			return errors.New("pubCoordinator: ack channel closed (stream ended)")
		}
		// Record client→hub transit RTT only on a successful ack, so a stalled or
		// failed batch does not contribute a spuriously large sample. Serial send
		// loop means this RTT is for exactly this batch.
		if pc.recordAckRTT != nil {
			pc.recordAckRTT(time.Since(sendStart))
		}
		auditOutboundForwards(pc.auditL, pc.peer, req.rawLines)
		close(req.doneCh)
		return nil
	case <-pc.stop:
		// Deliberate shutdown, not a delivery failure — do not count it.
		return errors.New("pubCoordinator: stopped while waiting for ack")
	case <-pc.stream.Context().Done():
		pc.sendFailed()
		return pc.stream.Context().Err()
	}
}

// sendFailed reports one in-flight batch that did not complete with an ack
// because the stream broke. The serial send loop fails at most one batch per
// disconnect, so this counts delivery disruptions, not idle reconnects.
func (pc *pubCoordinator) sendFailed() {
	if pc.recordSendFail != nil {
		pc.recordSendFail()
	}
}

// ackReader owns all reads on the Publish client stream. It forwards each
// PublishAck as a tick on ackCh and exits when the stream ends. The send loop
// guarantees only one batch is in-flight at a time, so ackCh capacity-1 is
// sufficient — there is never more than one outstanding ack to deliver.
func (pc *pubCoordinator) ackReader() {
	defer close(pc.ackDone)
	defer close(pc.ackCh)
	for {
		_, err := pc.stream.Recv()
		if err != nil {
			return
		}
		select {
		case pc.ackCh <- struct{}{}:
		case <-pc.stop:
			return
		}
	}
}
