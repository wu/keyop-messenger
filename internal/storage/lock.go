package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ErrDataDirLocked is returned by LockDataDir when another process already holds
// the data directory.
var ErrDataDirLocked = errors.New("data directory is in use by another process")

// lockFileName is the advisory lock file at the root of a data directory.
const lockFileName = ".lock"

// DataDirLock is an exclusive, process-wide claim on a data directory.
type DataDirLock struct {
	f *os.File
}

// LockDataDir takes an exclusive advisory lock on a data directory, creating it
// if needed. It returns ErrDataDirLocked if another process holds it.
//
// A data directory has exactly one writer by design: readers trust the committed
// end each channel's writer publishes, and a second process appending to the same
// segments makes that value wrong with nothing to detect it. The same assumption
// underlies subscriber offsets and compaction, which two processes would corrupt
// for each other. Holding the lock turns that silent corruption into a startup
// error.
//
// The lock is released by Unlock, and by the OS if the process dies — so a crash
// does not leave the directory unusable.
func LockDataDir(dataDir string) (*DataDirLock, error) {
	// #nosec G301 G703 -- dataDir is the operator-configured data directory, not
	// user input; 0o755 is appropriate for a shared data directory.
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory %q: %w", dataDir, err)
	}
	path := filepath.Join(dataDir, lockFileName)
	// #nosec G304 G703 -- path is this package's own filename under the
	// operator-configured data directory.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock file %q: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrDataDirLocked, dataDir)
		}
		return nil, fmt.Errorf("lock data directory %q: %w", dataDir, err)
	}
	return &DataDirLock{f: f}, nil
}

// Unlock releases the lock. The lock file itself is left in place: removing it
// would let a second process create and lock a new file at the same path while
// this one still held the old inode.
func (l *DataDirLock) Unlock() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	closeErr := l.f.Close()
	l.f = nil
	if err != nil {
		return fmt.Errorf("unlock data directory: %w", err)
	}
	return closeErr
}
