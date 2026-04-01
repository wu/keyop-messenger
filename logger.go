package messenger

// Logger is the structured logging interface callers inject into the Messenger.
// Its method set is compatible with [log/slog.Logger] for drop-in use:
//
//	m, _ := messenger.New(cfg, messenger.WithLogger(slog.Default()))
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// nopLogger discards all log output. Used when no Logger is provided.
type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}
