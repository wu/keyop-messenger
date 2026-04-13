package messenger_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wu/keyop-messenger"
)

// TestWithServiceName verifies that WithServiceName sets and WithServiceName
// retrieves the service name from the context.
func TestWithServiceName(t *testing.T) {
	ctx := context.Background()
	ctx = messenger.WithServiceName(ctx, "my-service")

	name := messenger.ServiceNameFromContext(ctx)
	assert.Equal(t, "my-service", name)
}

// TestServiceNameFromContext_Empty verifies that ServiceNameFromContext returns
// an empty string when the service name was not set.
func TestServiceNameFromContext_Empty(t *testing.T) {
	ctx := context.Background()
	name := messenger.ServiceNameFromContext(ctx)
	assert.Equal(t, "", name)
}

// TestWithServiceName_Overwrite verifies that calling WithServiceName twice
// overwrites the previous value.
func TestWithServiceName_Overwrite(t *testing.T) {
	ctx := context.Background()
	ctx = messenger.WithServiceName(ctx, "first-service")
	ctx = messenger.WithServiceName(ctx, "second-service")

	name := messenger.ServiceNameFromContext(ctx)
	assert.Equal(t, "second-service", name)
}

// TestWithServiceName_PreservesOtherValues verifies that setting a service name
// does not disturb other context values.
func TestWithServiceName_PreservesOtherValues(t *testing.T) {
	type ctxKey string
	const customKey ctxKey = "custom"

	ctx := context.Background()
	ctx = context.WithValue(ctx, customKey, "custom-value")
	ctx = messenger.WithServiceName(ctx, "my-service")

	// Both values should be present
	assert.Equal(t, "my-service", messenger.ServiceNameFromContext(ctx))
	assert.Equal(t, "custom-value", ctx.Value(customKey))
}

// TestServiceNameAndCorrelationID_Together verifies that service name and
// correlation ID can coexist in the same context without interfering.
func TestServiceNameAndCorrelationID_Together(t *testing.T) {
	ctx := context.Background()
	ctx = messenger.WithServiceName(ctx, "my-service")
	ctx = messenger.WithCorrelationID(ctx, "corr-123")

	assert.Equal(t, "my-service", messenger.ServiceNameFromContext(ctx))
	assert.Equal(t, "corr-123", messenger.CorrelationIDFromContext(ctx))
}

// TestServiceNameEmptyString verifies that an empty string is a valid service name.
func TestServiceNameEmptyString(t *testing.T) {
	ctx := context.Background()
	ctx = messenger.WithServiceName(ctx, "")

	name := messenger.ServiceNameFromContext(ctx)
	assert.Equal(t, "", name)
}

// TestServiceNameSpecialCharacters verifies that service names with special
// characters are handled correctly.
func TestServiceNameSpecialCharacters(t *testing.T) {
	ctx := context.Background()
	ctx = messenger.WithServiceName(ctx, "my-service_v1.0-prod")

	name := messenger.ServiceNameFromContext(ctx)
	assert.Equal(t, "my-service_v1.0-prod", name)
}

// TestServiceNameLarge verifies that large service name strings work.
func TestServiceNameLarge(t *testing.T) {
	largeName := string(make([]byte, 10000)) // 10KB of null bytes
	ctx := context.Background()
	ctx = messenger.WithServiceName(ctx, largeName)

	name := messenger.ServiceNameFromContext(ctx)
	assert.Equal(t, largeName, name)
}
