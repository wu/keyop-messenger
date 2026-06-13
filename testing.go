package messenger

// WithTestIdentity overrides the instance identity that [New] (and
// [NewEphemeralMessenger]) would normally derive from the local TLS
// certificate's CN. It exists so that library tests can exercise non-TLS code
// paths without provisioning real certificates.
//
// Production code MUST NOT call this. Federation peers identify each other by
// the TLS certificate CN; an instance name that does not match the cert breaks
// audit trails, per-peer offset files, and allowlist matching on the hub side.
func WithTestIdentity(name string) Option {
	return func(o *messengerOptions) {
		o.testIdentity = name
	}
}
