package messenger

import "time"

// messengerOptions holds construction-time settings that are not part of Config.
type messengerOptions struct {
	logger       Logger
	dataDir      string // overrides Config.Storage.DataDir when non-empty
	cfg          *Config
	testIdentity string // test-only override for the TLS-derived instance name
}

func defaultOptions() messengerOptions {
	return messengerOptions{
		logger: nopLogger{},
	}
}

// Option is a functional option for [New].
type Option func(*messengerOptions)

// WithLogger injects a structured logger. If not provided, log output is discarded.
// The Logger interface is compatible with [log/slog.Logger]:
//
//	messenger.New(cfg, messenger.WithLogger(slog.Default()))
func WithLogger(l Logger) Option {
	return func(o *messengerOptions) {
		o.logger = l
	}
}

// WithConfig supplies a Config via option rather than as the positional argument to New.
// When both are provided the positional argument takes precedence.
func WithConfig(cfg *Config) Option {
	return func(o *messengerOptions) {
		o.cfg = cfg
	}
}

// WithDataDir overrides Config.Storage.DataDir. Useful in tests or when the data
// directory is determined at runtime rather than in the config file.
func WithDataDir(dir string) Option {
	return func(o *messengerOptions) {
		o.dataDir = dir
	}
}

// subscribeOptions holds per-subscription settings supplied to Subscribe.
type subscribeOptions struct {
	maxAge time.Duration
}

// SubscribeOption configures a single [Messenger.Subscribe] call.
type SubscribeOption func(*subscribeOptions)

// WithMaxAge enables startup freshness filtering for this subscription. On its
// first run the subscriber skips any buffered messages older than d and begins
// delivery near real time, instead of replaying a stale backlog after a restart
// or reconnect. Filtering happens once at startup only — messages are never
// dropped mid-stream. 0 (default) disables it.
func WithMaxAge(d time.Duration) SubscribeOption {
	return func(o *subscribeOptions) {
		o.maxAge = d
	}
}
