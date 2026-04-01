// Package storage implements the per-channel file writer and related storage
// primitives. All file I/O is serialized through a single goroutine per channel
// so the OS O_APPEND guarantee covers records under PIPE_BUF; larger records
// are additionally protected by an advisory flock. This package targets POSIX
// systems (Linux, macOS) only.
package storage

// logger is the minimum logging interface required by this package.
// Structurally compatible with the root messenger.Logger and testutil.FakeLogger.
type logger interface {
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}
