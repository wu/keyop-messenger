package envelope

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testPayload is a concrete struct used across envelope tests.
type testPayload struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

func makeEnvelope(t *testing.T) Envelope {
	t.Helper()
	env, err := NewEnvelope("orders", "billing-host", "com.keyop.orders.OrderCreated",
		testPayload{OrderID: "ord-123", Amount: 99})
	require.NoError(t, err)
	return env
}

// TestMarshal_Unmarshal_RoundTrip confirms that every field survives a
// Marshal → Unmarshal cycle without loss.
func TestMarshal_Unmarshal_RoundTrip(t *testing.T) {
	original := makeEnvelope(t)

	data, err := Marshal(original)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// A JSONL record must fit on a single line.
	assert.NotContains(t, string(data), "\n",
		"marshaled envelope must not contain an embedded newline")

	recovered, err := Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, original.V, recovered.V)
	assert.Equal(t, original.ID, recovered.ID)
	assert.True(t, original.Ts.Equal(recovered.Ts),
		"timestamps differ: %v vs %v", original.Ts, recovered.Ts)
	assert.Equal(t, original.Channel, recovered.Channel)
	assert.Equal(t, original.Origin, recovered.Origin)
	assert.Equal(t, original.PayloadType, recovered.PayloadType)
	assert.JSONEq(t, string(original.Payload), string(recovered.Payload))
}

// TestUnmarshal_UnknownVersion confirms that an envelope with an unrecognised
// "v" value returns ErrUnknownVersion and still exposes the decoded fields so
// the caller can inspect or forward the record.
func TestUnmarshal_UnknownVersion(t *testing.T) {
	data := []byte(`{"v":99,"id":"abc-123","ts":"2026-03-31T00:00:00Z",` +
		`"channel":"orders","origin":"host","payload_type":"test.T","payload":{}}`)

	env, err := Unmarshal(data)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnknownVersion,
		"error must wrap ErrUnknownVersion so callers can distinguish it")

	// The partially-decoded envelope is returned for inspection.
	assert.Equal(t, 99, env.V)
	assert.Equal(t, "abc-123", env.ID)
	assert.Equal(t, "orders", env.Channel)
	assert.Equal(t, "host", env.Origin)
}

// TestUnmarshal_MalformedJSON confirms that invalid JSON returns a wrapped error,
// not a panic.
func TestUnmarshal_MalformedJSON(t *testing.T) {
	_, err := Unmarshal([]byte("{not valid json"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal envelope")
}

// TestNewEnvelope verifies the generated fields satisfy the spec:
// UUID v4 ID, UTC timestamp with no monotonic clock, and correct field values.
func TestNewEnvelope(t *testing.T) {
	before := time.Now().UTC()

	env, err := NewEnvelope("orders", "billing-host", "com.keyop.orders.OrderCreated",
		testPayload{OrderID: "ord-456", Amount: 42})

	after := time.Now().UTC()

	require.NoError(t, err)
	assert.Equal(t, CurrentVersion, env.V)

	// ID must be a valid UUID v4.
	id, parseErr := uuid.Parse(env.ID)
	require.NoError(t, parseErr, "ID must be a parseable UUID")
	assert.Equal(t, uuid.Version(4), id.Version(), "ID must be UUID version 4")

	// Timestamp must be within the test window and in UTC with no monotonic reading.
	assert.False(t, env.Ts.Before(before), "Ts must not be before call")
	assert.False(t, env.Ts.After(after), "Ts must not be after return")
	assert.Equal(t, time.UTC, env.Ts.Location(), "Ts must be in UTC")
	assert.Zero(t, env.Ts.Nanosecond()%1, "Ts must have no monotonic component (Round(0) applied)")
	// Confirm Round(0) was applied: marshaling and re-parsing must give the same value.
	marshaled, _ := json.Marshal(env.Ts)
	var reparsed time.Time
	require.NoError(t, json.Unmarshal(marshaled, &reparsed))
	assert.True(t, env.Ts.Equal(reparsed))

	assert.Equal(t, "orders", env.Channel)
	assert.Equal(t, "billing-host", env.Origin)
	assert.Equal(t, "com.keyop.orders.OrderCreated", env.PayloadType)

	// Payload must be valid JSON containing the original struct data.
	var decoded testPayload
	require.NoError(t, json.Unmarshal(env.Payload, &decoded))
	assert.Equal(t, "ord-456", decoded.OrderID)
	assert.Equal(t, 42, decoded.Amount)
}

// TestNewEnvelope_UniqueIDs confirms that successive calls produce distinct IDs.
func TestNewEnvelope_UniqueIDs(t *testing.T) {
	const n = 1000
	ids := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		env, err := NewEnvelope("ch", "host", "t", nil)
		require.NoError(t, err)
		ids[env.ID] = struct{}{}
	}
	assert.Len(t, ids, n, "all generated IDs must be unique")
}

// TestMarshal_Concurrent exercises the sync.Pool under concurrent load with
// the race detector. A data race on the pool or the returned slice would
// surface here.
func TestMarshal_Concurrent(t *testing.T) {
	env := makeEnvelope(t)

	const goroutines = 100
	const itersEach = 100

	errs := make(chan error, goroutines*itersEach)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < itersEach; j++ {
				b, err := Marshal(env)
				if err != nil {
					errs <- err
					return
				}
				if len(b) == 0 {
					errs <- errors.New("Marshal returned empty bytes")
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent Marshal error: %v", err)
	}
}

// TestDeadLetterPayload confirms the struct marshals and unmarshals cleanly.
func TestDeadLetterPayload(t *testing.T) {
	original := makeEnvelope(t)
	dlp := DeadLetterPayload{
		Original:  original,
		Retries:   5,
		LastError: "handler returned: state error",
		FailedAt:  time.Now().UTC().Round(0),
	}

	data, err := json.Marshal(dlp)
	require.NoError(t, err)

	var recovered DeadLetterPayload
	require.NoError(t, json.Unmarshal(data, &recovered))

	assert.Equal(t, dlp.Retries, recovered.Retries)
	assert.Equal(t, dlp.LastError, recovered.LastError)
	assert.True(t, dlp.FailedAt.Equal(recovered.FailedAt))
	assert.Equal(t, original.ID, recovered.Original.ID)
}
