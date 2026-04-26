// Package messenger is a Go pub-sub library with persistent message storage, federation,
// and at-least-once delivery semantics.
//
// See the servicename subpackage documentation for context helpers that stamp service names on messages.
// Service names identify which service published a message and are useful for debugging and log triage.
package messenger

import "context"

type serviceNameKey struct{}

// WithServiceName returns a new context with the service name set.
// The service name is typically the name of the service that is publishing messages.
func WithServiceName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, serviceNameKey{}, name)
}

// ServiceNameFromContext retrieves the service name from the context.
// Returns an empty string if the service name was not set.
func ServiceNameFromContext(ctx context.Context) string {
	val, ok := ctx.Value(serviceNameKey{}).(string)
	if !ok {
		return ""
	}
	return val
}
