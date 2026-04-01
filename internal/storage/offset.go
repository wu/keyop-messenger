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
	data, err := os.ReadFile(path)
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

	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create tmp offset file: %w", err)
	}
	if _, err = fmt.Fprintf(f, "%d\n", offset); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write offset: %w", err)
	}
	if err = f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("fsync offset file: %w", err)
	}
	if err = f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close offset file: %w", err)
	}
	if err = os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename offset file: %w", err)
	}
	return nil
}

// OffsetFileExists reports whether an offset file exists at path.
func OffsetFileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
