//go:build integration

package messenger

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/envelope"
	"google.golang.org/grpc"
)

// receivedMsg is what a test subscriber saw: the message and the headers on
// the context its handler was called with.
type receivedMsg struct {
	msg        Message
	ctxHeaders map[string]string
}

// subscribeHeaders subscribes to channel and returns the messages delivered.
func subscribeHeaders(t *testing.T, m *Messenger, channel string) <-chan receivedMsg {
	t.Helper()
	received := make(chan receivedMsg, 10)
	subCtx, subCancel := context.WithCancel(context.Background())
	t.Cleanup(subCancel)
	require.NoError(t, m.Subscribe(subCtx, channel, "sub-1",
		func(ctx context.Context, msg Message) error {
			received <- receivedMsg{msg: msg, ctxHeaders: HeadersFromContext(ctx)}
			return nil
		},
	))
	return received
}

func nextReceived(t *testing.T, received <-chan receivedMsg) receivedMsg {
	t.Helper()
	select {
	case r := <-received:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
		return receivedMsg{}
	}
}

// TestMessenger_Headers_PublishAndSubscribe verifies that headers set with
// WithHeader are stamped on the envelope, survive the trip through the
// channel file, and reach the subscriber in Message.Headers but not on the
// handler's context.
func TestMessenger_Headers_PublishAndSubscribe(t *testing.T) {
	t.Parallel()
	m, err := newForTest(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	registerTestTypes(t, m)
	received := subscribeHeaders(t, m, "events")

	pubCtx := WithHeader(context.Background(), "tags", "temp,metricmon")
	pubCtx = WithHeader(pubCtx, "other", "x")
	require.NoError(t, m.Publish(pubCtx, "events", "test.Event", map[string]string{"key": "value"}))

	r := nextReceived(t, received)
	assert.Equal(t, map[string]string{"tags": "temp,metricmon", "other": "x"}, r.msg.Headers)
	assert.Nil(t, r.ctxHeaders, "delivery must not put the message's headers on the handler context")
}

// TestMessenger_Headers_Empty verifies that a message published without
// headers has nil Headers.
func TestMessenger_Headers_Empty(t *testing.T) {
	t.Parallel()
	m, err := newForTest(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	registerTestTypes(t, m)
	received := subscribeHeaders(t, m, "events")

	require.NoError(t, m.Publish(context.Background(), "events", "test.Event", map[string]string{"key": "value"}))

	assert.Nil(t, nextReceived(t, received).msg.Headers)
}

// TestMessenger_Headers_PublishBatch verifies that every message in a batch
// carries the context's headers.
func TestMessenger_Headers_PublishBatch(t *testing.T) {
	t.Parallel()
	m, err := newForTest(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	registerTestTypes(t, m)
	received := subscribeHeaders(t, m, "events")

	pubCtx := WithHeader(context.Background(), "tags", "temp")
	require.NoError(t, m.PublishBatch(pubCtx, "events", []BatchMessage{
		{PayloadType: "test.Event", Payload: map[string]string{"n": "1"}},
		{PayloadType: "test.Event", Payload: map[string]string{"n": "2"}},
	}))

	for range 2 {
		assert.Equal(t, map[string]string{"tags": "temp"}, nextReceived(t, received).msg.Headers)
	}
}

// TestMessenger_Headers_CarriedByCaller verifies the hop-by-hop pattern the
// messenger leaves to the application: a handler reads the received headers,
// sets the ones to keep on its context, and publishes with it.
func TestMessenger_Headers_CarriedByCaller(t *testing.T) {
	t.Parallel()
	m, err := newForTest(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	registerTestTypes(t, m)

	subCtx, subCancel := context.WithCancel(context.Background())
	t.Cleanup(subCancel)
	require.NoError(t, m.Subscribe(subCtx, "metrics", "relay",
		func(ctx context.Context, msg Message) error {
			ctx = WithHeader(ctx, "tags", msg.Headers["tags"]+",metricmon")
			return m.Publish(ctx, "status", "test.Event", msg.Payload)
		},
	))
	received := subscribeHeaders(t, m, "status")

	pubCtx := WithHeader(context.Background(), "tags", "temp")
	require.NoError(t, m.Publish(pubCtx, "metrics", "test.Event", map[string]string{"key": "value"}))

	assert.Equal(t, map[string]string{"tags": "temp,metricmon"}, nextReceived(t, received).msg.Headers)
}

// TestEphemeralMessenger_Publish_Headers verifies that the ephemeral publish
// path stamps the context's headers on the envelope it sends to the hub.
func TestEphemeralMessenger_Publish_Headers(t *testing.T) {
	records := make(chan []byte, 1)
	addr := startEphemeralHub(t, &mockEphemeralGRPCHub{
		publishFn: func(stream grpc.BidiStreamingServer[federationv1.PublishBatch, federationv1.PublishAck]) error {
			for {
				batch, err := stream.Recv()
				if err != nil {
					return nil
				}
				for _, rec := range batch.Records {
					select {
					case records <- rec:
					default:
					}
				}
				_ = stream.Send(&federationv1.PublishAck{})
			}
		},
	})

	em, err := newEphemeralForTest(EphemeralConfig{HubAddr: addr})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))

	pubCtx := WithHeader(ctx, "tags", "temp")
	require.NoError(t, em.Publish(pubCtx, "events", "test.Event", map[string]string{"k": "v"}))

	select {
	case rec := <-records:
		env, err := envelope.Unmarshal(rec)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"tags": "temp"}, env.Headers)
	case <-time.After(2 * time.Second):
		t.Fatal("hub never received the publish frame")
	}
}

// TestEphemeralMessenger_Subscribe_Headers verifies that headers on an
// envelope pushed by the hub reach the ephemeral subscriber.
func TestEphemeralMessenger_Subscribe_Headers(t *testing.T) {
	env, err := envelope.NewEnvelope("events", "hub", "test.Msg", map[string]string{"x": "1"})
	require.NoError(t, err)
	env.Headers = map[string]string{"tags": "temp,metricmon"}
	raw, err := envelope.Marshal(env)
	require.NoError(t, err)

	addr := startEphemeralHub(t, &mockEphemeralGRPCHub{
		subscribeFn: func(stream grpc.BidiStreamingServer[federationv1.SubscribeFrame, federationv1.HubBatch]) error {
			if _, err := stream.Recv(); err != nil { // read SubscribeRequest
				return nil
			}
			_ = stream.Send(&federationv1.HubBatch{
				Payload: &federationv1.HubBatch_Batch{
					Batch: &federationv1.EnvelopeBatch{Records: [][]byte{raw}},
				},
			})
			_, _ = stream.Recv() // read ack
			<-stream.Context().Done()
			return nil
		},
	})

	em, err := newEphemeralForTest(EphemeralConfig{HubAddr: addr, Subscribe: []string{"events"}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })
	registerTestTypes(t, em)

	received := make(chan Message, 1)
	require.NoError(t, em.Subscribe("events", func(msg Message) { received <- msg }))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, em.Connect(ctx))

	select {
	case msg := <-received:
		assert.Equal(t, map[string]string{"tags": "temp,metricmon"}, msg.Headers)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}
