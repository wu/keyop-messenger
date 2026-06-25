package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalNotifier_SendAndReceive(t *testing.T) {
	n := NewLocalNotifier()

	n.Notify()
	select {
	case <-n.C():
	default:
		t.Fatal("expected notification on C()")
	}
}

func TestLocalNotifier_Coalescing(t *testing.T) {
	n := NewLocalNotifier()

	// Multiple Notify calls must not block and must collapse to one token.
	n.Notify()
	n.Notify()
	n.Notify()

	count := 0
	for {
		select {
		case <-n.C():
			count++
		default:
			goto done
		}
	}
done:
	assert.Equal(t, 1, count, "multiple Notify calls must produce exactly one token")
}
