// Package servicename provides context helpers for stamping service names on messages.
//
// Service names identify which service published a message. They are useful for
// debugging and log triage, allowing operators to quickly identify the source of
// a message even when multiple services publish to the same channel on the same instance.
//
// Set a service name in context before publishing, and it will be automatically
// stamped on the envelope and delivered to subscribers:
//
//	ctx := messenger.WithServiceName(context.Background(), "payment-processor")
//	messenger.Publish(ctx, "payments", "event.type", payload)
//
// Subscribers receive the service name in the Message:
//
//	messenger.Subscribe(ctx, "payments", "sub-id", func(ctx context.Context, msg Message) error {
//	    slog.Info("message", "service", msg.ServiceName)
//	    return nil
//	})
//
// Service names are preserved across hub forwarding, making them available for
// tracing in distributed message flows.
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
