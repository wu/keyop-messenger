package dedup

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLRUDedup_NewID(t *testing.T) {
	d, err := NewLRUDedup(100)
	require.NoError(t, err)
	assert.False(t, d.SeenOrAdd("id-1"), "first call must return false (not yet seen)")
}

func TestLRUDedup_DuplicateID(t *testing.T) {
	d, err := NewLRUDedup(100)
	require.NoError(t, err)
	d.SeenOrAdd("id-1")
	assert.True(t, d.SeenOrAdd("id-1"), "second call must return true (already seen)")
}

func TestLRUDedup_Eviction(t *testing.T) {
	const size = 10
	d, err := NewLRUDedup(size)
	require.NoError(t, err)

	// Fill the cache with size+1 unique IDs; the first one should be evicted.
	for i := 0; i <= size; i++ {
		d.SeenOrAdd(fmt.Sprintf("id-%d", i))
	}

	// The oldest entry ("id-0") should have been evicted.
	assert.False(t, d.SeenOrAdd("id-0"), "evicted ID must return false (treated as new)")
}

func TestLRUDedup_Race(t *testing.T) {
	const (
		goroutines = 100
		ids        = 1000
	)
	d, err := NewLRUDedup(100_000)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < ids; i++ {
				d.SeenOrAdd(fmt.Sprintf("g%d-id%d", g, i))
			}
		}()
	}
	wg.Wait()
}

func BenchmarkSeenOrAdd(b *testing.B) {
	d, err := NewLRUDedup(100_000)
	require.NoError(b, err)

	ids := make([]string, b.N)
	for i := range ids {
		ids[i] = fmt.Sprintf("id-%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.SeenOrAdd(ids[i])
	}
}
