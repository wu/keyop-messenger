package federation_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"testing"

	"github.com/keyop/keyop-messenger/internal/federation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// roundTrip encodes records with WriteFrame then decodes with ReadFrame.
func roundTrip(t *testing.T, records [][]byte, maxBytes int) [][]byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, federation.WriteFrame(&buf, records))
	got, err := federation.ReadFrame(&buf, maxBytes)
	require.NoError(t, err)
	return got
}

func TestFrameRoundTripEmpty(t *testing.T) {
	got := roundTrip(t, [][]byte{}, 0)
	assert.Empty(t, got)
}

func TestFrameRoundTripSingle(t *testing.T) {
	rec := []byte(`{"id":"abc","channel":"test"}`)
	got := roundTrip(t, [][]byte{rec}, 0)
	require.Len(t, got, 1)
	assert.Equal(t, rec, got[0])
}

func TestFrameRoundTripHundredRecords(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	records := make([][]byte, 100)
	for i := range records {
		size := rng.Intn(500) + 10
		records[i] = make([]byte, size)
		rng.Read(records[i])
	}

	got := roundTrip(t, records, 0)
	require.Len(t, got, 100)
	for i, rec := range got {
		assert.Equal(t, records[i], rec, "record %d mismatch", i)
	}
}

func TestFrameRoundTripZeroLengthRecord(t *testing.T) {
	got := roundTrip(t, [][]byte{{}}, 0)
	require.Len(t, got, 1)
	assert.Equal(t, []byte{}, got[0])
}

func TestFrameTooLarge(t *testing.T) {
	big := make([]byte, 1024)
	var buf bytes.Buffer
	require.NoError(t, federation.WriteFrame(&buf, [][]byte{big}))

	_, err := federation.ReadFrame(&buf, 512)
	assert.ErrorIs(t, err, federation.ErrFrameTooLarge)
}

func TestFrameMaxBytesZeroMeansNoLimit(t *testing.T) {
	big := make([]byte, 1<<20) // 1 MB
	got := roundTrip(t, [][]byte{big}, 0)
	require.Len(t, got, 1)
	assert.Len(t, got[0], 1<<20)
}

func TestFrameTruncatedLengthPrefix(t *testing.T) {
	// Only 2 bytes of a 4-byte length prefix.
	buf := bytes.NewReader([]byte{0x00, 0x01})
	_, err := federation.ReadFrame(buf, 0)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestFrameTruncatedRecordBody(t *testing.T) {
	// Length prefix claims 100 bytes but only 50 follow.
	var buf bytes.Buffer
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, 100)
	buf.Write(hdr)
	buf.Write(make([]byte, 50))

	_, err := federation.ReadFrame(&buf, 0)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestFrameReaderReturnsUnexpectedEOF(t *testing.T) {
	// A reader that returns io.ErrUnexpectedEOF partway through; ReadFrame must
	// propagate it.
	r := &errReader{data: []byte{0x00, 0x00}, err: io.ErrUnexpectedEOF}
	_, err := federation.ReadFrame(r, 0)
	assert.True(t, errors.Is(err, io.ErrUnexpectedEOF), "expected ErrUnexpectedEOF, got %v", err)
}

// errReader yields data then returns err on the next call.
type errReader struct {
	data []byte
	err  error
	done bool
}

func (e *errReader) Read(p []byte) (int, error) {
	if e.done || len(e.data) == 0 {
		return 0, e.err
	}
	n := copy(p, e.data)
	e.data = e.data[n:]
	if len(e.data) == 0 {
		e.done = true
	}
	return n, nil
}
