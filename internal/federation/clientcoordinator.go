package federation

import (
	"errors"
	"sync"

	"github.com/gorilla/websocket"
)

// clientCoordinator serialises hub→peer WebSocket sends across all channel
// readers attached to one peer connection. One instance is created per
// connected peer that has subscriptions.
//
// A single goroutine dequeues sendReqs from requestCh, writes the binary
// WebSocket frame, waits for the text-frame ack routed from the PeerReceiver
// via ackCh, then closes req.doneCh to unblock the originating channelReader.
// Sequential delivery is guaranteed by the single goroutine model.
type clientCoordinator struct {
	conn          *websocket.Conn
	connWriteMu   *sync.Mutex   // shared with PeerReceiver to serialise all writes
	ackCh         <-chan AckMsg // acks routed from PeerReceiver (owned by hub side)
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
// write to the same queue.
func newClientCoordinator(
	conn *websocket.Conn,
	connWriteMu *sync.Mutex,
	ackCh <-chan AckMsg,
	maxBatchBytes int,
	log logger,
	readers []*channelReader,
) *clientCoordinator {
	requestCh := make(chan sendReq, 1)
	cc := &clientCoordinator{
		conn:          conn,
		connWriteMu:   connWriteMu,
		ackCh:         ackCh,
		maxBatchBytes: maxBatchBytes,
		log:           log,
		readers:       readers,
		requestCh:     requestCh,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	// Wire the shared requestCh into every reader.
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
// waits for everything to exit. The offset files on disk are intentionally left
// in place — they are needed by the compactor and the TTL sweep.
//
// Safe to call once. Callers must ensure close() is not invoked concurrently.
func (cc *clientCoordinator) close() {
	select {
	case <-cc.stop:
	default:
		close(cc.stop)
	}
	// Wait for the coordinator goroutine first: it may be mid-send or waiting for
	// an ack. Once it exits, no new doneCh closures will race with reader shutdown.
	<-cc.done
	// Stop all readers. Any reader blocked on <-doneCh will unblock via <-cr.stop.
	for _, r := range cc.readers {
		r.close()
	}
}

func (cc *clientCoordinator) run() {
	defer close(cc.done)
	for {
		select {
		case <-cc.stop:
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

// sendBatch writes one batch to the WebSocket connection and waits for the ack.
// Returns a non-nil error on connection or ack failure; the caller (run) will
// exit on error, triggering the hub cleanup path.
func (cc *clientCoordinator) sendBatch(req sendReq) error {
	// Write the binary frame holding all raw JSONL lines.
	cc.connWriteMu.Lock()
	w, err := cc.conn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		cc.connWriteMu.Unlock()
		return err
	}
	if err := WriteFrame(w, req.rawLines); err != nil {
		_ = w.Close()
		cc.connWriteMu.Unlock()
		return err
	}
	if err := w.Close(); err != nil {
		cc.connWriteMu.Unlock()
		return err
	}
	cc.connWriteMu.Unlock()

	// Wait for the text-frame ack routed from the PeerReceiver.
	select {
	case _, ok := <-cc.ackCh:
		if !ok {
			return errors.New("clientCoordinator: ack channel closed (connection lost)")
		}
		// Ack received; signal the channelReader to persist its offset.
		close(req.doneCh)
		return nil
	case <-cc.stop:
		return errors.New("clientCoordinator: stopped while waiting for ack")
	}
}
