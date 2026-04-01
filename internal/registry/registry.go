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

// PayloadRegistry maps type discriminator strings to Go types.
type PayloadRegistry interface {
	// Register associates typeStr with the Go type of prototype. If prototype
	// is a pointer, the element type is stored. Registering the same typeStr
	// twice returns ErrPayloadTypeAlreadyRegistered.
	Register(typeStr string, prototype any) error

	// Decode unmarshals raw into the registered type for typeStr and returns it
	// as an interface value. If typeStr is not registered, raw is decoded into
	// map[string]any, a warning is logged, and (map, nil) is returned — the
	// message is never dropped.
	Decode(typeStr string, raw json.RawMessage) (any, error)

	// KnownTypes returns all registered type strings in sorted order.
	KnownTypes() []string
}

// logger is the minimum logging interface required by this package.
// It is structurally compatible with the root messenger.Logger interface and
// with testutil.FakeLogger, so callers need not import this package's type.
type logger interface {
	Warn(msg string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Warn(string, ...any) {}

// defaultRegistry is the production implementation of PayloadRegistry.
type defaultRegistry struct {
	mu      sync.RWMutex
	entries map[string]reflect.Type
	log     logger
}

// New returns a ready-to-use PayloadRegistry. log may be nil, in which case
// warning output is discarded.
func New(log logger) PayloadRegistry {
	if log == nil {
		log = nopLogger{}
	}
	return &defaultRegistry{
		entries: make(map[string]reflect.Type),
		log:     log,
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
		// Unknown type: decode into a generic map and warn. The message is
		// never dropped; the caller receives map[string]any.
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("decode unregistered payload %q: %w", typeStr, err)
		}
		r.log.Warn("unregistered payload type; delivering as map[string]any",
			"payload_type", typeStr)
		return m, nil
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
