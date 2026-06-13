package federation

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandlerGate_RejectsAfterClose verifies that handlerEnter returns false
// once the gate is closed, and that handlerWg.Wait blocks until every
// pre-close call to handlerEnter has been matched by handlerExit.
func TestHandlerGate_RejectsAfterClose(t *testing.T) {
	h := &Hub{}

	require.True(t, h.handlerEnter(), "handlerEnter should succeed before close")

	// Mirror what Hub.Close does: set the flag under the mutex.
	h.handlerGateMu.Lock()
	h.handlerGateClosed = true
	h.handlerGateMu.Unlock()

	assert.False(t, h.handlerEnter(), "handlerEnter should fail after gate closed")

	// Wait must block while the outstanding enter has not yet exited.
	waited := make(chan struct{})
	go func() {
		h.handlerWg.Wait()
		close(waited)
	}()
	select {
	case <-waited:
		t.Fatal("handlerWg.Wait returned before handlerExit was called")
	case <-time.After(20 * time.Millisecond):
	}

	h.handlerExit()

	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("handlerWg.Wait did not return after handlerExit")
	}
}

// TestHandlerGate_NoConcurrentRace is a race-detector regression for the
// Add/Wait race on Hub.handlerWg. It fires many goroutines that call
// handlerEnter concurrently with the gate closing and handlerWg.Wait being
// called — the sequence that produced DATA RACE under the old direct Add/Wait
// pattern.
func TestHandlerGate_NoConcurrentRace(_ *testing.T) {
	h := &Hub{}

	const n = 100
	start := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			<-start
			if h.handlerEnter() {
				h.handlerExit()
			}
		}()
	}

	// Close the gate and call Wait concurrently with the goroutines above.
	h.handlerGateMu.Lock()
	h.handlerGateClosed = true
	h.handlerGateMu.Unlock()
	close(start)

	h.handlerWg.Wait() // old code: DATA RACE here; new code: safe
	wg.Wait()
}
