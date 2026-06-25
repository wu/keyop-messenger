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
	maxAge          time.Duration
	maxRetries      *int // nil = use the instance's subscribers.max_retries
	retryBackoffSet bool
	retryBase       time.Duration
	retryMax        time.Duration
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

// WithMaxRetries overrides subscribers.max_retries for this subscription only —
// the number of times a failing handler is retried before the message is
// dead-lettered. The instance config value is used when this option is omitted.
// A negative value is treated as 0 (dead-letter on the first failure).
//
// Use it to give one channel a different retry budget than the instance default:
// e.g. a short budget on a latency-sensitive channel that tolerates reordering, or
// a long one on an ordered channel that must ride out a downstream restart. Pair
// it with WithRetryBackoff to also widen the per-attempt delays.
func WithMaxRetries(n int) SubscribeOption {
	return func(o *subscribeOptions) {
		o.maxRetries = &n
	}
}

// WithRetryBackoff overrides the exponential-backoff schedule for this
// subscription only: the first retry waits base, doubling each attempt, capped at
// maxDelay. The instance config (subscribers.retry_backoff_base_ms /
// retry_backoff_max_ms) is used when this option is omitted. A non-positive base or
// maxDelay falls back to the built-in default (100ms base, 5s cap), not the
// instance config.
func WithRetryBackoff(base, maxDelay time.Duration) SubscribeOption {
	return func(o *subscribeOptions) {
		o.retryBackoffSet = true
		o.retryBase = base
		o.retryMax = maxDelay
	}
}
