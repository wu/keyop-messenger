package messenger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestWithCorrelationID sets a correlation ID in context and retrieves it.
func TestWithCorrelationID(t *testing.T) {
	ctx := context.Background()
	id := "test-correlation-123"

	ctx = WithCorrelationID(ctx, id)
	retrieved := CorrelationIDFromContext(ctx)

	assert.Equal(t, id, retrieved)
}

// TestCorrelationIDFromContext_Empty returns empty string when no correlation ID is set.
func TestCorrelationIDFromContext_Empty(t *testing.T) {
	ctx := context.Background()
	retrieved := CorrelationIDFromContext(ctx)

	assert.Equal(t, "", retrieved)
}

// TestWithCorrelationID_Overwrite overwrites a previously set correlation ID.
func TestWithCorrelationID_Overwrite(t *testing.T) {
	ctx := context.Background()
	id1 := "first-id"
	id2 := "second-id"

	ctx = WithCorrelationID(ctx, id1)
	assert.Equal(t, id1, CorrelationIDFromContext(ctx))

	ctx = WithCorrelationID(ctx, id2)
	assert.Equal(t, id2, CorrelationIDFromContext(ctx))
}

// TestWithCorrelationID_PreservesOtherValues ensures adding correlation ID
// doesn't overwrite other context values.
func TestWithCorrelationID_PreservesOtherValues(t *testing.T) {
	type testKey struct{}
	testValue := "test-value"

	ctx := context.WithValue(context.Background(), testKey{}, testValue)
	ctx = WithCorrelationID(ctx, "corr-id")

	assert.Equal(t, "corr-id", CorrelationIDFromContext(ctx))
	assert.Equal(t, testValue, ctx.Value(testKey{}))
}
