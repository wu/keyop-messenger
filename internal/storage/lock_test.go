//nolint:gosec // test file: G302 (deliberate directory permissions)
package storage

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLockDataDir(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "data")

	lock, err := LockDataDir(dir)
	require.NoError(t, err)
	assert.DirExists(t, dir, "the data directory is created if absent")
	assert.FileExists(t, filepath.Join(dir, lockFileName))

	require.NoError(t, lock.Unlock())

	// A released directory can be claimed again — a clean restart must work.
	again, err := LockDataDir(dir)
	require.NoError(t, err)
	require.NoError(t, again.Unlock())

	// Unlocking twice is not an error.
	assert.NoError(t, again.Unlock())
}

// TestLockDataDir_SecondClaimIsRefused verifies the guard actually excludes a
// second claim. flock associates a lock with the open file description, so a
// separate open of the same path is refused — which is what a second process
// does, and what would otherwise silently interleave writes and invalidate every
// channel's committed end.
func TestLockDataDir_SecondClaimIsRefused(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "data")

	first, err := LockDataDir(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Unlock() })

	second, err := LockDataDir(dir)
	if second != nil {
		t.Cleanup(func() { _ = second.Unlock() })
	}
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDataDirLocked)
	assert.ErrorContains(t, err, dir, "the error must name the directory in use")
}

func TestLockDataDir_UnwritableParent(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	parent := t.TempDir()
	require.NoError(t, os.Chmod(parent, 0o500))
	t.Cleanup(func() { _ = os.Chmod(parent, 0o750) })

	_, err := LockDataDir(filepath.Join(parent, "data"))
	require.Error(t, err)
}

// TestLockDataDir_ReclaimedAfterProcessDeath demonstrates why the lock holds no
// PID and needs no staleness check. flock belongs to the open file description,
// so the kernel releases it when the holder dies by any means — and after a
// machine crash there are no open file descriptions at all. The .lock file left
// behind on disk is inert.
//
// A PID file would need the opposite: written on start, checked on restart, and
// wrong whenever the PID has been reused.
func TestLockDataDir_ReclaimedAfterProcessDeath(t *testing.T) {
	if dir := os.Getenv("KEYOP_TEST_LOCK_DIR"); dir != "" {
		// Child: take the lock, announce it, then wait to be killed without ever
		// unlocking — exactly what a crash looks like.
		if _, err := LockDataDir(dir); err != nil {
			fmt.Println("ERR", err)
			os.Exit(1)
		}
		fmt.Println("locked")
		select {}
	}

	dir := filepath.Join(t.TempDir(), "data")
	cmd := exec.Command(os.Args[0], "-test.run=^TestLockDataDir_ReclaimedAfterProcessDeath$") // #nosec G204 -- re-exec of this test binary
	cmd.Env = append(os.Environ(), "KEYOP_TEST_LOCK_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	line, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "locked\n", line, "child did not take the lock")

	// While the child holds it, we cannot.
	_, err = LockDataDir(dir)
	require.ErrorIs(t, err, ErrDataDirLocked)

	// Kill it outright: no deferred Unlock, no signal handler, no cleanup.
	require.NoError(t, cmd.Process.Kill())
	_, _ = cmd.Process.Wait()

	require.Eventually(t, func() bool {
		lock, err := LockDataDir(dir)
		if err != nil {
			return false
		}
		_ = lock.Unlock()
		return true
	}, 5*time.Second, 10*time.Millisecond,
		"a killed process's lock must be reclaimable without any staleness check")

	assert.FileExists(t, filepath.Join(dir, lockFileName),
		"the lock file survives; it is the lock that does not")
}
