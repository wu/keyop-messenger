package messenger

import (
	"bytes"
	"os/exec"
	"testing"
)

// TestGolangCILint runs golangci-lint and fails the test if any issues are found.
func TestGolangCILint(t *testing.T) {
	cmd := exec.Command("golangci-lint", "run", "./...")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("golangci-lint failed or not found: %v\n\nOutput:\n%s", err, out.String())
	}
}
