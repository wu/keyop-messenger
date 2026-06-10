package federation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReceiveAck_ClosedChannel verifies that receiveAck returns an error when
// the ackCh is closed (signals connection loss from the PeerReceiver).
func TestReceiveAck_ClosedChannel(t *testing.T) {
	ackCh := make(chan AckMsg)
	ps := &PeerSender{ackCh: ackCh}

	close(ackCh)

	_, err := ps.receiveAck()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ack channel closed")
}

// TestReceiveAck_ChannelHappyPath verifies that receiveAck returns the ack
// delivered on ackCh when the channel is open.
func TestReceiveAck_ChannelHappyPath(t *testing.T) {
	ackCh := make(chan AckMsg, 1)
	ps := &PeerSender{ackCh: ackCh}

	ackCh <- AckMsg{LastID: "msg-abc"}

	ack, err := ps.receiveAck()
	require.NoError(t, err)
	assert.Equal(t, "msg-abc", ack.LastID)
}
