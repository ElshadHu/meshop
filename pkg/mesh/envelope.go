package mesh

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// Envelope wraps a single message exchanged between two peers.
type Envelope struct {
	// ID is a message identifier
	ID string `json:"id"`
	// From is the sender's PeerID
	From PeerID `json:"from"`
	// To is the recipient's PeerID
	To PeerID `json:"to"`
	// Type  tell the receiver how to dispatch the message
	Type string `json:"type"`
	// Timestamp is the sender's local clock time at send
	Timestamp time.Time `json:"timestamp"`
	// Payload is application data. encoding/json transmits []byte as
	// base64 inside the JSON envelope
	Payload []byte `json:"payload"`
	// Nonce is the AEAD nonce. It is set by session
	Nonce []byte `json:"nonce,omitempty"`
}

// envelopeIDBytes is the number of random bytes in ID
const envelopeIDBytes = 16

// NewEnvelope builds an Envelope with a fresh random ID and the current
// local time. Nonce stays empty; Session populates it on Send
func NewEnvelope(from, to PeerID, msgType string, payload []byte) (Envelope, error) {
	b := make([]byte, envelopeIDBytes)
	if _, err := rand.Read(b); err != nil {
		return Envelope{}, fmt.Errorf("mesh: generate envelope id: %w", err)
	}

	return Envelope{
		ID:        hex.EncodeToString(b),
		From:      from,
		To:        to,
		Type:      msgType,
		Timestamp: time.Now(),
		Payload:   payload,
	}, nil
}
