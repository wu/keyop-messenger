package federation

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
)

// Message type constants for the federation wire protocol.
const (
	MsgTypeJSON   = 1 // JSON frame: handshake or ack
	MsgTypeBinary = 2 // binary frame: length-prefixed record batch
	msgTypeClose  = 3 // close frame: [2-byte big-endian code][reason text]
)

// CloseError is returned by ReadMessage / NextReader when a close frame arrives.
type CloseError struct {
	Code int
	Text string
}

func (e *CloseError) Error() string {
	if e.Text != "" {
		return fmt.Sprintf("federation: connection closed %d: %s", e.Code, e.Text)
	}
	return fmt.Sprintf("federation: connection closed %d", e.Code)
}

// stringAddr is a minimal net.Addr backed by static strings.
type stringAddr struct {
	network string
	address string
}

func (a *stringAddr) Network() string { return a.network }
func (a *stringAddr) String() string  { return a.address }

// Conn is a full-duplex message connection over a pair of io.Reader / io.Writer
// streams (typically an HTTP/2 request body + response writer).
//
// Wire format per message: [1-byte type][4-byte big-endian uint32 payload length][payload]
//
// Conn does not serialise concurrent writes internally; callers must use an
// external mutex (connWriteMu) when multiple goroutines write to the same Conn,
// preserving the same pattern used with the previous gorilla/websocket connection.
type Conn struct {
	r       io.Reader
	w       io.Writer
	addr    net.Addr
	closeFn func() error
	flushFn func() // called after each write; auto-detected from w

	closeOnce sync.Once
}

// NewConn creates a Conn. r is the read direction, w is the write direction.
// addr is the remote address (may be nil). closeFn is called by Close(); nil is
// safe. If w implements http.Flusher, Flush is called automatically after each
// write so that HTTP/2 DATA frames reach the peer without buffering delay.
func NewConn(r io.Reader, w io.Writer, addr net.Addr, closeFn func() error) *Conn {
	c := &Conn{r: r, w: w, addr: addr}
	if closeFn != nil {
		c.closeFn = closeFn
	} else {
		c.closeFn = func() error { return nil }
	}
	if f, ok := w.(http.Flusher); ok {
		c.flushFn = f.Flush
	}
	return c
}

// RemoteAddr returns the remote address, or nil if unknown.
func (c *Conn) RemoteAddr() net.Addr { return c.addr }

// WriteMessage writes a single complete message. Header and payload are
// combined into one Write call so pipe-backed connections deliver them atomically.
func (c *Conn) WriteMessage(msgType int, data []byte) error {
	buf := make([]byte, 5+len(data))
	buf[0] = byte(msgType)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(data)))
	copy(buf[5:], data)
	if _, err := c.w.Write(buf); err != nil {
		return fmt.Errorf("federation: conn write: %w", err)
	}
	if c.flushFn != nil {
		c.flushFn()
	}
	return nil
}

// WriteClose sends a close frame with code and text, then calls Close.
func (c *Conn) WriteClose(code int, text string) error {
	payload := make([]byte, 2+len(text))
	binary.BigEndian.PutUint16(payload[:2], uint16(code))
	copy(payload[2:], text)
	err := c.WriteMessage(msgTypeClose, payload)
	_ = c.Close()
	return err
}

// ReadMessage reads the next complete message.
// Returns (msgType, payload, nil) for data/JSON messages.
// Returns (0, nil, *CloseError) when a close frame arrives.
// Returns (0, nil, err) for I/O errors.
func (c *Conn) ReadMessage() (msgType int, data []byte, err error) {
	var hdr [5]byte
	if _, err = io.ReadFull(c.r, hdr[:]); err != nil {
		return 0, nil, fmt.Errorf("federation: conn read header: %w", err)
	}
	t := hdr[0]
	n := int(binary.BigEndian.Uint32(hdr[1:5]))

	var payload []byte
	if n > 0 {
		payload = make([]byte, n)
		if _, err = io.ReadFull(c.r, payload); err != nil {
			return 0, nil, fmt.Errorf("federation: conn read payload: %w", err)
		}
	}

	if t == msgTypeClose {
		ce := &CloseError{}
		if len(payload) >= 2 {
			ce.Code = int(binary.BigEndian.Uint16(payload[:2]))
			ce.Text = string(payload[2:])
		}
		return 0, nil, ce
	}
	return int(t), payload, nil
}

// NextReader reads the next complete message and returns a reader over its
// payload. The payload is buffered eagerly so the underlying stream is always
// fully consumed regardless of whether the caller reads the returned reader.
// Returns (0, nil, *CloseError) when a close frame arrives.
func (c *Conn) NextReader() (msgType int, r io.Reader, err error) {
	var hdr [5]byte
	if _, err = io.ReadFull(c.r, hdr[:]); err != nil {
		return 0, nil, fmt.Errorf("federation: conn next reader: %w", err)
	}
	t := hdr[0]
	n := int(binary.BigEndian.Uint32(hdr[1:5]))

	var payload []byte
	if n > 0 {
		payload = make([]byte, n)
		if _, err = io.ReadFull(c.r, payload); err != nil {
			return 0, nil, fmt.Errorf("federation: conn read payload: %w", err)
		}
	}

	if t == msgTypeClose {
		ce := &CloseError{}
		if len(payload) >= 2 {
			ce.Code = int(binary.BigEndian.Uint16(payload[:2]))
			ce.Text = string(payload[2:])
		}
		return 0, nil, ce
	}
	return int(t), bytes.NewReader(payload), nil
}

// NextWriter returns a WriteCloser that buffers writes and sends the complete
// message atomically when Close is called. Callers must hold connWriteMu
// (if shared with another goroutine) for the entire NextWriter → Close sequence.
func (c *Conn) NextWriter(msgType int) (io.WriteCloser, error) {
	return &msgWriter{conn: c, msgType: msgType}, nil
}

// Close closes the connection once. Subsequent calls are no-ops.
func (c *Conn) Close() error {
	var err error
	c.closeOnce.Do(func() { err = c.closeFn() })
	return err
}

// msgWriter buffers writes and sends the complete framed message when closed.
type msgWriter struct {
	conn    *Conn
	msgType int
	buf     []byte
	closed  bool
}

func (w *msgWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *msgWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.conn.WriteMessage(w.msgType, w.buf)
}
