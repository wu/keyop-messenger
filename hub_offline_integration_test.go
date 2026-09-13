//go:build integration

package messenger

import (
	"net"
	"testing"
)

// TestNew_HubUnreachable_StartsOffline verifies that New succeeds when the
// configured hub is not listening, so an instance can start offline.
func TestNew_HubUnreachable_StartsOffline(t *testing.T) {
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "client-a")

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}

	// newClientMessengerWithPolicy fails the test if New returns an error.
	newClientMessengerWithPolicy(t, "client-a", dir, caFile,
		certFor("client-a"), keyFor("client-a"), addr,
		nil, []string{"events"},
		WithHubFatalHandler(func(hubAddr string, err error) {
			t.Errorf("unexpected hub fatal error for %s: %v", hubAddr, err)
		}),
	)
}
