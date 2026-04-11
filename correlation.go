package messenger

import "context"

type correlationIDKey struct{}

// WithCorrelationID returns a context carrying the given correlation ID.
// Pass the returned context to Publish to stamp all resulting messages with this ID.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, id)
}

// CorrelationIDFromContext returns the correlation ID stored in ctx, or "" if none.
func CorrelationIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(correlationIDKey{}).(string)
	return id
}
