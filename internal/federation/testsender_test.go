//go:build integration

package federation_test

import (
	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/envelope"
)

// testSender is a minimal Publish-stream driver used by integration tests that
// act as a raw client against a hub. It marshals envelopes, sends them as
// PublishBatch frames, and waits for the corresponding PublishAck. Replaces
// the production PeerSender removed when client-side publishing switched to
// disk-pull.
type testSender struct {
	stream federationv1.FederationService_PublishClient
}

func newTestSender(stream federationv1.FederationService_PublishClient) *testSender {
	return &testSender{stream: stream}
}

// Send marshals env, sends one PublishBatch containing it, and blocks for the
// PublishAck. Returns the marshal/send/recv error if any step fails.
func (s *testSender) Send(env *envelope.Envelope) error {
	data, err := envelope.Marshal(*env)
	if err != nil {
		return err
	}
	if err := s.stream.Send(&federationv1.PublishBatch{Records: [][]byte{data}}); err != nil {
		return err
	}
	_, err = s.stream.Recv()
	return err
}
