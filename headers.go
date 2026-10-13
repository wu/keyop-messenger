package messenger

import "context"

type headersKey struct{}

// WithHeader returns a context carrying header key set to value, alongside any
// headers ctx already carries. Pass the returned context to Publish to stamp
// the headers on every resulting message. An empty value removes key.
//
// The messenger gives headers no meaning, and a subscriber's handler context
// does not carry the received message's headers: whether a header moves on to
// the messages a handler publishes is the application's choice. Read them from
// Message.Headers and set the ones to keep with WithHeader.
func WithHeader(ctx context.Context, key, value string) context.Context {
	old, _ := ctx.Value(headersKey{}).(map[string]string)
	headers := make(map[string]string, len(old)+1)
	for k, v := range old {
		headers[k] = v
	}
	if value == "" {
		delete(headers, key)
	} else {
		headers[key] = value
	}
	return context.WithValue(ctx, headersKey{}, headers)
}

// HeadersFromContext returns a copy of the headers stored in ctx, or nil if
// there are none.
func HeadersFromContext(ctx context.Context) map[string]string {
	stored, _ := ctx.Value(headersKey{}).(map[string]string)
	if len(stored) == 0 {
		return nil
	}
	headers := make(map[string]string, len(stored))
	for k, v := range stored {
		headers[k] = v
	}
	return headers
}
