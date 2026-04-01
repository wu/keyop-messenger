package messenger

// messengerOptions holds construction-time settings that are not part of Config.
type messengerOptions struct {
	logger  Logger
	dataDir string // overrides Config.Storage.DataDir when non-empty
	cfg     *Config
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
