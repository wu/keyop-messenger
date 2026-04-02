package federation

import (
	"encoding/json"
	"fmt"

	"github.com/gorilla/websocket"
)

// HandshakeMsg is exchanged as the first text frame on every new connection,
// by both hub peers and clients.
type HandshakeMsg struct {
	InstanceName string `json:"instance_name"`
	Role         string `json:"role"`              // "hub" or "client"
	Version      string `json:"version"`           // wire protocol version, currently "1"
	LastID       string `json:"last_id,omitempty"` // last acked ID; non-empty on reconnect
}

// AckMsg is sent by the receiver after processing a batch of data frames.
// LastID is the envelope ID of the last successfully processed message,
// used to drive replay on reconnect.
type AckMsg struct {
	LastID string `json:"last_id"`
}

// SendHandshake writes msg to conn as a JSON-encoded WebSocket text frame.
func SendHandshake(conn *websocket.Conn, msg HandshakeMsg) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("federation: marshal handshake: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("federation: send handshake: %w", err)
	}
	return nil
}

// ReceiveHandshake reads the next WebSocket message from conn and decodes it
// as a HandshakeMsg. Returns an error if the frame is not a text frame.
func ReceiveHandshake(conn *websocket.Conn) (HandshakeMsg, error) {
	var msg HandshakeMsg
	msgType, data, err := conn.ReadMessage()
	if err != nil {
		return msg, fmt.Errorf("federation: receive handshake: %w", err)
	}
	if msgType != websocket.TextMessage {
		return msg, fmt.Errorf("federation: handshake: expected text frame, got type %d", msgType)
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return msg, fmt.Errorf("federation: unmarshal handshake: %w", err)
	}
	return msg, nil
}

// SendAck writes ack to conn as a JSON-encoded WebSocket text frame.
func SendAck(conn *websocket.Conn, ack AckMsg) error {
	data, err := json.Marshal(ack)
	if err != nil {
		return fmt.Errorf("federation: marshal ack: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("federation: send ack: %w", err)
	}
	return nil
}

// ReceiveAck reads the next WebSocket message from conn and decodes it as an
// AckMsg. Returns an error if the frame is not a text frame.
func ReceiveAck(conn *websocket.Conn) (AckMsg, error) {
	var ack AckMsg
	msgType, data, err := conn.ReadMessage()
	if err != nil {
		return ack, fmt.Errorf("federation: receive ack: %w", err)
	}
	if msgType != websocket.TextMessage {
		return ack, fmt.Errorf("federation: ack: expected text frame, got type %d", msgType)
	}
	if err := json.Unmarshal(data, &ack); err != nil {
		return ack, fmt.Errorf("federation: unmarshal ack: %w", err)
	}
	return ack, nil
}
