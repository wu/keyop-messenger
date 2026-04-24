// Package federation implements the hub-to-hub and hub-to-client wire protocol.
//
//nolint:gosec // G115: safe type conversions (uintptr->int, int->uint32)
package federation

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

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
// is placed in skipped rather than records. Its raw bytes are preserved so the
// caller can unmarshal the envelope ID and send an ack — ensuring the sender
// advances past the oversized message rather than retrying it indefinitely.
//
// If the byte stream is truncated mid-record or mid-header, ReadFrame returns
// an error wrapping io.ErrUnexpectedEOF.
func ReadFrame(r io.Reader, maxRecordBytes int) (records [][]byte, skipped [][]byte, err error) {
	data, readErr := io.ReadAll(r)
	if readErr != nil {
		return nil, nil, fmt.Errorf("federation: read frame: %w", readErr)
	}

	for len(data) > 0 {
		if len(data) < 4 {
			return nil, nil, fmt.Errorf("federation: truncated length prefix: %w", io.ErrUnexpectedEOF)
		}
		n := int(binary.BigEndian.Uint32(data[:4]))
		data = data[4:]

		if len(data) < n {
			return nil, nil, fmt.Errorf("federation: truncated record body: %w", io.ErrUnexpectedEOF)
		}

		rec := make([]byte, n)
		copy(rec, data[:n])
		data = data[n:]

		if maxRecordBytes > 0 && n > maxRecordBytes {
			skipped = append(skipped, rec)
		} else {
			records = append(records, rec)
		}
	}
	return records, skipped, nil
}
