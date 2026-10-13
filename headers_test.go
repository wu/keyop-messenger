package messenger_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wu/keyop-messenger"
)

// TestWithHeader verifies that headers set with WithHeader are returned by
// HeadersFromContext.
func TestWithHeader(t *testing.T) {
	ctx := messenger.WithHeader(context.Background(), "tags", "temp,metricmon")
	ctx = messenger.WithHeader(ctx, "other", "x")

	assert.Equal(t, map[string]string{"tags": "temp,metricmon", "other": "x"},
		messenger.HeadersFromContext(ctx))
}

// TestHeadersFromContext_Empty verifies that a context without headers
// returns nil, so an envelope stamped from it omits the field.
func TestHeadersFromContext_Empty(t *testing.T) {
	assert.Nil(t, messenger.HeadersFromContext(context.Background()))
}

// TestWithHeader_Overwrite verifies that setting a header twice keeps the
// second value.
func TestWithHeader_Overwrite(t *testing.T) {
	ctx := messenger.WithHeader(context.Background(), "tags", "temp")
	ctx = messenger.WithHeader(ctx, "tags", "temp,metricmon")

	assert.Equal(t, map[string]string{"tags": "temp,metricmon"}, messenger.HeadersFromContext(ctx))
}

// TestWithHeader_EmptyValueRemoves verifies that an empty value removes the
// header, and that removing the last one leaves no headers.
func TestWithHeader_EmptyValueRemoves(t *testing.T) {
	ctx := messenger.WithHeader(context.Background(), "tags", "temp")
	ctx = messenger.WithHeader(ctx, "tags", "")

	assert.Nil(t, messenger.HeadersFromContext(ctx))
}

// TestWithHeader_ParentUnchanged verifies that setting a header on a derived
// context does not change the headers of the context it came from.
func TestWithHeader_ParentUnchanged(t *testing.T) {
	parent := messenger.WithHeader(context.Background(), "tags", "temp")
	_ = messenger.WithHeader(parent, "tags", "temp,metricmon")
	_ = messenger.WithHeader(parent, "other", "x")

	assert.Equal(t, map[string]string{"tags": "temp"}, messenger.HeadersFromContext(parent))
}

// TestHeadersFromContext_ReturnsCopy verifies that changing the returned map
// does not change the headers stored in the context.
func TestHeadersFromContext_ReturnsCopy(t *testing.T) {
	ctx := messenger.WithHeader(context.Background(), "tags", "temp")

	got := messenger.HeadersFromContext(ctx)
	got["tags"] = "changed"
	got["other"] = "x"

	assert.Equal(t, map[string]string{"tags": "temp"}, messenger.HeadersFromContext(ctx))
}

// TestWithHeader_PreservesOtherValues verifies that setting a header keeps
// the other values carried by the context.
func TestWithHeader_PreservesOtherValues(t *testing.T) {
	ctx := messenger.WithServiceName(context.Background(), "my-service")
	ctx = messenger.WithCorrelationID(ctx, "corr-1")
	ctx = messenger.WithHeader(ctx, "tags", "temp")

	assert.Equal(t, "my-service", messenger.ServiceNameFromContext(ctx))
	assert.Equal(t, "corr-1", messenger.CorrelationIDFromContext(ctx))
}
