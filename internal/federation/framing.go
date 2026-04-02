// Package federation implements the hub-to-hub and hub-to-client wire protocol.
package federation

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ErrFrameTooLarge is returned by ReadFrame when a record's declared length
// exceeds the configured maxRecordBytes limit.
var ErrFrameTooLarge = errors.New("federation: record exceeds max batch bytes")

// WriteFrame encodes records using the length-prefix scheme
// [4-byte big-endian uint32 length][record bytes] and writes the result to w
// in a single call. The caller is responsible for opening and closing the
// underlying WebSocket message writer.
func WriteFrame(w io.Writer, records [][]byte) error {
	var buf bytes.Buffer
	hdr := make([]byte, 4)
	for _, rec := range records {
		binary.BigEndian.PutUint32(hdr, uint32(len(rec)))
		buf.Write(hdr)
		buf.Write(rec)
	}
	_, err := w.Write(buf.Bytes())
	return err
}

// ReadFrame reads all bytes from r and splits them into records using the
// same length-prefix scheme used by WriteFrame. r is expected to be the
// reader returned by websocket.Conn.NextReader for a single binary message,
// so it yields exactly one frame's worth of bytes before EOF.
//
// If maxRecordBytes > 0, any record whose declared length exceeds that limit
// returns ErrFrameTooLarge. If the byte stream is truncated mid-record or
// mid-header, ReadFrame returns an error wrapping io.ErrUnexpectedEOF.
func ReadFrame(r io.Reader, maxRecordBytes int) ([][]byte, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("federation: read frame: %w", err)
	}

	var records [][]byte
	for len(data) > 0 {
		if len(data) < 4 {
			return nil, fmt.Errorf("federation: truncated length prefix: %w", io.ErrUnexpectedEOF)
		}
		n := int(binary.BigEndian.Uint32(data[:4]))
		data = data[4:]

		if maxRecordBytes > 0 && n > maxRecordBytes {
			return nil, ErrFrameTooLarge
		}
		if len(data) < n {
			return nil, fmt.Errorf("federation: truncated record body: %w", io.ErrUnexpectedEOF)
		}

		rec := make([]byte, n)
		copy(rec, data[:n])
		records = append(records, rec)
		data = data[n:]
	}
	return records, nil
}
