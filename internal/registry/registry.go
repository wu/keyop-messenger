// Package registry maps payload type discriminator strings to Go types for
// encoding and decoding message payloads.
package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
)

// ErrPayloadTypeAlreadyRegistered is returned by Register when the same type
// string is registered more than once.
var ErrPayloadTypeAlreadyRegistered = errors.New("payload type already registered")

// ErrUnregisteredPayloadType is returned by Decode when typeStr has no
// registered Go type. The registry no longer coerces unknown payloads into
// map[string]any: that fallback silently lost any non-object payload (a JSON
// array, string, or number fails to unmarshal into a map, which surfaced as a
// decode error that the subscriber skipped) and bypassed the dead-letter queue.
// Callers decide how to handle this error — the durable subscriber dead-letters
// the message; the ephemeral subscriber skips it with a warning.
var ErrUnregisteredPayloadType = errors.New("unregistered payload type")

// PayloadRegistry maps type discriminator strings to Go types.
type PayloadRegistry interface {
	// Register associates typeStr with the Go type of prototype. If prototype
	// is a pointer, the element type is stored. Registering the same typeStr
	// twice returns ErrPayloadTypeAlreadyRegistered.
	Register(typeStr string, prototype any) error

	// Decode unmarshals raw into the registered type for typeStr and returns it
	// as a value. It returns ErrUnregisteredPayloadType (with a nil value) when
	// typeStr is not registered, or a wrapped decode error when raw does not
	// unmarshal into the registered type.
	Decode(typeStr string, raw json.RawMessage) (any, error)

	// KnownTypes returns all registered type strings in sorted order.
	KnownTypes() []string
}

// defaultRegistry is the production implementation of PayloadRegistry. It is a
// pure codec with no side effects: Decode returns errors rather than logging,
// leaving logging and dead-letter/skip policy to the caller.
type defaultRegistry struct {
	mu      sync.RWMutex
	entries map[string]reflect.Type
}

// New returns a ready-to-use PayloadRegistry.
func New() PayloadRegistry {
	return &defaultRegistry{
		entries: make(map[string]reflect.Type),
	}
}

// Register implements PayloadRegistry.
func (r *defaultRegistry) Register(typeStr string, prototype any) error {
	t := reflect.TypeOf(prototype)
	if t == nil {
		return fmt.Errorf("register %q: prototype must not be nil", typeStr)
	}
	// If the caller passes a pointer, store the element type so that Decode
	// always returns a value (not a pointer), keeping the API consistent.
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.entries[typeStr]; exists {
		return fmt.Errorf("%w: %q", ErrPayloadTypeAlreadyRegistered, typeStr)
	}
	r.entries[typeStr] = t
	return nil
}

// Decode implements PayloadRegistry.
func (r *defaultRegistry) Decode(typeStr string, raw json.RawMessage) (any, error) {
	r.mu.RLock()
	t, ok := r.entries[typeStr]
	r.mu.RUnlock()

	if !ok {
		// Unknown type: no Go type to decode into. Return a typed error so the
		// caller can dead-letter or skip; never silently coerce into a map (which
		// lost non-object payloads entirely).
		return nil, fmt.Errorf("%w: %q", ErrUnregisteredPayloadType, typeStr)
	}

	// Registered type: allocate a new *T, unmarshal into it, return T (value).
	ptr := reflect.New(t)
	if err := json.Unmarshal(raw, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("decode payload %q: %w", typeStr, err)
	}
	return ptr.Elem().Interface(), nil
}

// KnownTypes implements PayloadRegistry.
func (r *defaultRegistry) KnownTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.entries))
	for k := range r.entries {
		types = append(types, k)
	}
	sort.Strings(types)
	return types
}
