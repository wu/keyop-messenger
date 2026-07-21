package storage

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ReadOffset reads the byte offset stored at path. The file must contain a
// decimal integer as written by WriteOffset.
func ReadOffset(path string) (int64, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- trusted, library-constructed offset file path
	if err != nil {
		return 0, fmt.Errorf("read offset file %q: %w", path, err)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse offset in %q: %w", path, err)
	}
	return n, nil
}

// WriteOffset atomically persists offset to path. It writes to a sibling .tmp
// file, fsyncs, then renames over path so the original is never left in a
// half-written state.
func WriteOffset(path string, offset int64) error {
	tmp := path + ".tmp"

	f, err := os.Create(tmp) // #nosec G304 -- trusted, library-constructed offset file path
	if err != nil {
		return fmt.Errorf("create tmp offset file: %w", err)
	}
	if _, err = fmt.Fprintf(f, "%d\n", offset); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write offset: %w", err)
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("fsync offset file: %w", err)
	}
	if err = f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close offset file: %w", err)
	}
	if err = os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename offset file: %w", err)
	}
	return nil
}

// OffsetFileExists reports whether an offset file exists at path.
func OffsetFileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// SanitizeForFilename returns a filename-safe form of s, replacing every
// character outside [a-zA-Z0-9._-] with '_'. It is used to derive offset
// filenames from caller-controlled identifiers (subscriber IDs, peer CNs, hub
// addresses) that are otherwise joined straight into a path. Because '/' (and
// the platform separator) is replaced, the result can never introduce a path
// component, so it cannot escape its intended directory. The transform is
// idempotent: applying it to already-sanitized input is a no-op. An empty input
// yields an empty string (callers always append a fixed suffix such as
// ".offset", so the basename is never "." or "..").
func SanitizeForFilename(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '.', c == '_', c == '-':
			out[i] = c
		default:
			out[i] = '_'
		}
	}
	return string(out)
}
