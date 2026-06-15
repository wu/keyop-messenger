package audit_test

import (
	"io"
	"os"
	"testing"

	"github.com/wu/keyop-messenger/internal/audit"
)

func TestMain(m *testing.M) {
	audit.SetStderr(io.Discard)
	os.Exit(m.Run())
}
