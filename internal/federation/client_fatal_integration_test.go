//go:build integration

package federation

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// TestClientConnectWithReconnect_HubDown_StartsDisconnected verifies that an
// unreachable hub does not fail ConnectWithReconnect, and that the client
// connects once the hub comes up.
func TestClientConnectWithReconnect_HubDown_StartsDisconnected(t *testing.T) {
	t.Parallel()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	client := newTestClient(&testutil.FakeLogger{}, nil)
	t.Cleanup(client.Close)
	var fatalCalled atomic.Bool
	client.SetOnFatal(func(error) { fatalCalled.Store(true) })

	require.NoError(t, client.ConnectWithReconnect(addr), "an unreachable hub must not fail startup")
	assert.False(t, client.Connected())

	lis, err = net.Listen("tcp", addr)
	require.NoError(t, err)
	grpcSrv := grpc.NewServer()
	federationv1.RegisterFederationServiceServer(grpcSrv, &mockFedServer{})
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcSrv.Stop)

	require.Eventually(t, client.Connected, 10*time.Second, 20*time.Millisecond)
	assert.Zero(t, client.ReconnectCount(), "the first connection is not a reconnect")
	assert.False(t, fatalCalled.Load())
}

// TestClientConnectWithReconnect_FatalFirstDial_ReturnsError verifies that a
// non-retryable first-dial failure is still returned.
func TestClientConnectWithReconnect_FatalFirstDial_ReturnsError(t *testing.T) {
	t.Parallel()
	f := newTLSFixture(t)
	addr := startTLSHub(t, f.config("hub", f.ca2Cert, f.ca2Key, f.ca1Cert))
	c := newTLSTestClient(t, f.config("client", f.ca1Cert, f.ca1Key, f.ca1Cert))

	err := c.ConnectWithReconnect(addr)
	require.Error(t, err)
	assert.True(t, isFatalConnErr(err), "untrusted hub cert must be fatal: %v", err)
}

// TestClientConnectWithReconnect_HubRejection_CallsOnFatal verifies that a hub
// rejecting the stream after it opens invokes OnFatal once and stops the
// reconnect loop.
func TestClientConnectWithReconnect_HubRejection_CallsOnFatal(t *testing.T) {
	t.Parallel()
	var conns atomic.Int32
	addr := startMockServer(t, &mockFedServer{
		publishFn: func(grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			conns.Add(1)
			return status.Error(codes.PermissionDenied, "not in allowlist")
		},
	})

	client := newTestClient(&testutil.FakeLogger{}, nil)
	t.Cleanup(client.Close)
	fatal := make(chan error, 2)
	client.SetOnFatal(func(err error) { fatal <- err })

	require.NoError(t, client.ConnectWithReconnect(addr), "the rejection arrives after the stream opens")

	select {
	case err := <-fatal:
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("OnFatal was not called")
	}

	// Longer than the test client's reconnect backoff.
	time.Sleep(time.Second)
	assert.Equal(t, int32(1), conns.Load(), "the client must not reconnect after a fatal error")
	assert.Empty(t, fatal, "OnFatal must be called once")
	assert.False(t, client.Connected())
}
