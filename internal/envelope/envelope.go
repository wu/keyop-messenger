// Package envelope defines the message envelope written to every channel file.
package envelope

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// CurrentVersion is the envelope schema version written by this build.
const CurrentVersion = 1

// ErrUnknownVersion is returned by Unmarshal when the envelope's "v" field does
// not equal CurrentVersion. The caller receives the partially-decoded Envelope
// and may inspect its fields or fall back to the raw bytes it already holds.
var ErrUnknownVersion = errors.New("unknown envelope version")

// Envelope is the fixed-schema wrapper written as a single JSON line to a
// channel file. Application-specific data lives entirely in Payload.
type Envelope struct {
	// V is the schema version. Always CurrentVersion for envelopes written by
	// this build. Readers must tolerate unknown versions gracefully.
	V int `json:"v"`

	// ID is a UUID v4 that uniquely identifies this message. Used for
	// deduplication across hubs; it is not the instance identifier.
	ID string `json:"id"`

	// Ts is the publish timestamp in UTC, formatted as RFC3339Nano.
	Ts time.Time `json:"ts"`

	// Channel is the name of the channel this message was published to.
	Channel string `json:"channel"`

	// Origin is the instance name of the original publisher, preserved across
	// hub forwarding.
	Origin string `json:"origin"`

	// PayloadType is a fully-qualified type discriminator for the payload,
	// e.g. "com.keyop.orders.OrderCreated". Reverse-DNS format is recommended.
	PayloadType string `json:"payload_type"`

	// CorrelationID is an optional application-level identifier used to group
	// related messages across a multi-step process. Omitted from JSON if empty.
	CorrelationID string `json:"correlation_id,omitempty"`

	// ServiceName is the name of the service that published this message.
	// Preserved across hub forwarding. Omitted from JSON if empty.
	ServiceName string `json:"service_name,omitempty"`

	// Payload is the application-defined body. Its structure is described by
	// PayloadType and decoded via the payload registry.
	Payload json.RawMessage `json:"payload"`
}

// DeadLetterPayload is the payload written to a dead-letter channel when a
// subscriber's handler exhausts its retry budget.
type DeadLetterPayload struct {
	// Original is the envelope that could not be processed.
	Original Envelope `json:"original"`

	// Retries is the number of delivery attempts that were made.
	Retries int `json:"retries"`

	// LastError is the string representation of the final handler error or panic.
	LastError string `json:"last_error"`

	// FailedAt is when the final retry failed.
	FailedAt time.Time `json:"failed_at"`
}

// bufPool recycles byte buffers across Marshal calls to reduce allocations.
var bufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// Marshal encodes env as a single JSON object with no trailing newline.
// It is safe to call concurrently.
func Marshal(env Envelope) ([]byte, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()

	if err := json.NewEncoder(buf).Encode(env); err != nil {
		bufPool.Put(buf)
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}

	// json.Encoder.Encode appends '\n'; trim it so the caller controls line endings.
	b := buf.Bytes()
	if len(b) > 0 && b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
	}

	// Copy before returning the buffer to the pool — another goroutine may
	// grab and reset it the moment Put is called.
	result := make([]byte, len(b))
	copy(result, b)

	bufPool.Put(buf)
	return result, nil
}

// Unmarshal decodes a single JSON line into an Envelope.
//
// If the "v" field does not equal CurrentVersion, Unmarshal returns the
// partially-decoded Envelope together with an error wrapping ErrUnknownVersion.
// Callers should log the error and continue rather than dropping the record;
// the original raw bytes are still available for further inspection.
func Unmarshal(data []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, fmt.Errorf("unmarshal envelope: %w", err)
	}
	if env.V != CurrentVersion {
		return env, fmt.Errorf("%w: %d", ErrUnknownVersion, env.V)
	}
	return env, nil
}

// NewEnvelope constructs a ready-to-publish Envelope. It generates a UUID v4
// message ID, stamps the current UTC time, and marshals payload into JSON.
//
// channel is the destination channel name.
// origin is the instance name of the publishing instance.
// payloadType is the type discriminator string (e.g. "com.keyop.orders.OrderCreated").
// payload is any JSON-serialisable value.
func NewEnvelope(channel, origin, payloadType string, payload any) (Envelope, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return Envelope{}, fmt.Errorf("generate message ID: %w", err)
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal payload: %w", err)
	}

	return Envelope{
		V:           CurrentVersion,
		ID:          id.String(),
		Ts:          time.Now().UTC().Round(0), // Round(0) strips the monotonic clock reading
		Channel:     channel,
		Origin:      origin,
		PayloadType: payloadType,
		Payload:     json.RawMessage(raw),
	}, nil
}
