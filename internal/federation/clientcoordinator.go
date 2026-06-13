package federation

import (
	"errors"

	"google.golang.org/grpc"

	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
)

// clientCoordinator serialises hub→peer sends across all channel readers
// attached to one peer connection. One instance is created per connected peer
// that has subscriptions.
//
// A single goroutine dequeues sendReqs from requestCh, sends the EnvelopeBatch
// via the gRPC Subscribe server stream, waits for a signal on ackCh (written by
// the ack-reader goroutine in the Subscribe handler), then closes req.doneCh to
// unblock the originating channelReader. Sequential delivery is guaranteed by
// the single-goroutine model.
//
// The coordinator is the sole writer on the Subscribe server stream; the ack-reader
// goroutine in the Subscribe handler is the sole reader. Concurrent Send/Recv on a
// gRPC server stream is safe per the gRPC-Go guarantees.
type clientCoordinator struct {
	stream        grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]
	ackCh         <-chan struct{} // closed or signalled by the Subscribe handler's ack-reader goroutine
	maxBatchBytes int
	log           logger
	readers       []*channelReader
	requestCh     chan sendReq

	stop chan struct{}
	done chan struct{}
}

// newClientCoordinator constructs a clientCoordinator. The coordinator does not
// start any goroutines until start() is called.
//
// requestCh is created here and assigned to each channelReader so they all
// write to the same queue. ackCh is closed by the caller's ack-reader goroutine
// when the Subscribe stream ends.
func newClientCoordinator(
	stream grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch],
	ackCh <-chan struct{},
	maxBatchBytes int,
	log logger,
	readers []*channelReader,
) *clientCoordinator {
	requestCh := make(chan sendReq, 1)
	cc := &clientCoordinator{
		stream:        stream,
		ackCh:         ackCh,
		maxBatchBytes: maxBatchBytes,
		log:           log,
		readers:       readers,
		requestCh:     requestCh,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	for _, r := range readers {
		r.requestCh = requestCh
	}
	return cc
}

// start launches all channel reader goroutines and the coordinator send goroutine.
func (cc *clientCoordinator) start() {
	for _, r := range cc.readers {
		r.start()
	}
	go cc.run()
}

// close stops the coordinator send goroutine and all channel readers, then
// waits for everything to exit.
func (cc *clientCoordinator) close() {
	select {
	case <-cc.stop:
	default:
		close(cc.stop)
	}
	<-cc.done
	for _, r := range cc.readers {
		r.close()
	}
}

func (cc *clientCoordinator) run() {
	defer close(cc.done)
	streamDone := cc.stream.Context().Done()
	for {
		select {
		case <-cc.stop:
			return
		case <-streamDone:
			return
		case req, ok := <-cc.requestCh:
			if !ok {
				return
			}
			if err := cc.sendBatch(req); err != nil {
				cc.log.Error("clientCoordinator: send batch failed",
					"channel", req.channel, "err", err)
				return
			}
		}
	}
}

// sendBatch sends one EnvelopeBatch to the peer and waits for the ack.
// Returns a non-nil error on stream or ack failure; run() exits on error.
func (cc *clientCoordinator) sendBatch(req sendReq) error {
	if err := cc.stream.Send(&federationv1.HubBatch{
		Payload: &federationv1.HubBatch_Batch{
			Batch: &federationv1.EnvelopeBatch{Records: req.rawLines},
		},
	}); err != nil {
		return err
	}

	select {
	case _, ok := <-cc.ackCh:
		if !ok {
			return errors.New("clientCoordinator: ack channel closed (stream ended)")
		}
		close(req.doneCh)
		return nil
	case <-cc.stop:
		return errors.New("clientCoordinator: stopped while waiting for ack")
	case <-cc.stream.Context().Done():
		return cc.stream.Context().Err()
	}
}
