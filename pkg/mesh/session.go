package mesh

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/flynn/noise"
)

// nonceBytes is the AEAD nonce length
const nonceBytes = 12

// Session is an authenticated, encrypted channel between two peers
// Concurrency contract:
//   - Send is safe to call from multiple goroutines simultaneously.
//   - Recv is NOT safe for concurrent use; call from a single goroutine.
//   - Close may be called from any goroutine.
type Session struct {
	node     *Node
	sendCS   *noise.CipherState
	recvCS   *noise.CipherState
	remoteID PeerID
}

// RemoteID returns the PeerID of the remote peer
func (s *Session) RemoteID() PeerID { return s.remoteID }

// LocalID returns this peer's PeerID
func (s *Session) LocalID() PeerID { return s.node.ID() }

// Close tears down the session and closes the underlying connection
func (s *Session) Close() error { return s.node.Close() }

// Send encrypts env.Payload, populates env.Nonce, and writes the
// resulting envelope to the wire
func (s *Session) Send(ctx context.Context, env Envelope) error {
	s.node.sendMu.Lock()
	defer s.node.sendMu.Unlock()
	nonceCounter := s.sendCS.Nonce()
	env.Nonce = encodeNonce(nonceCounter)
	aad := envelopeAAD(env)
	cipherText, err := s.sendCS.Encrypt(nil, aad, env.Payload)
	if err != nil {
		return fmt.Errorf("mesh: session send: encrypt: %w", err)
	}
	env.Payload = cipherText
	return s.node.sendEnvelopeLocked(ctx, env)
}

// Recv reads the next envelope from the wire, verifies the wire
// nonce matches the next expected counter
func (s *Session) Recv(ctx context.Context) (Envelope, error) {
	env, err := s.node.Recv(ctx)
	if err != nil {
		return Envelope{}, err
	}

	if len(env.Nonce) != nonceBytes {
		return Envelope{}, fmt.Errorf("mesh: session recv: bad nonce length %d", len(env.Nonce))
	}
	got, err := decodeNonce(env.Nonce)
	if err != nil {
		return Envelope{}, fmt.Errorf("mesh: session recv: %w", err)
	}
	want := s.recvCS.Nonce()
	if got != want {
		return Envelope{}, fmt.Errorf("mesh: session recv: nonce mismatch (got %d, want %d)", got, want)
	}
	aad := envelopeAAD(env)
	plaintext, err := s.recvCS.Decrypt(nil, aad, env.Payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("mesh: session recv: decrypt: %w", err)
	}
	env.Payload = plaintext
	return env, nil
}

// envelopeAAD returns deterministic associated-data bytes for env.
// The Payload field is excluded (it is the AEAD plaintext) every
// other field is bound, including the Nonce
func envelopeAAD(env Envelope) []byte {
	out := make([]byte, 0, 256)
	out = appendLP(out, []byte(env.ID))
	out = appendLP(out, []byte(env.From))
	out = appendLP(out, []byte(env.To))
	out = appendLP(out, []byte(env.Type))
	out = binary.BigEndian.AppendUint64(out, uint64(env.Timestamp.UnixNano()))
	out = appendLP(out, env.Nonce)
	return out
}

func appendLP(out, b []byte) []byte {
	out = binary.BigEndian.AppendUint32(out, uint32(len(b)))
	return append(out, b...)
}

// encodeNonce produces a 12 byte big endian wire nonce from a uint64 counter
func encodeNonce(counter uint64) []byte {
	out := make([]byte, nonceBytes)
	binary.BigEndian.PutUint64(out[4:], counter)
	return out
}

// decodeNonce reverses encodeNonce
func decodeNonce(b []byte) (uint64, error) {
	if len(b) != nonceBytes {
		return 0, fmt.Errorf("mesh: nonce length %d, want %d", len(b), nonceBytes)
	}

	for i := 0; i < 4; i++ {
		if b[i] != 0 {
			return 0, fmt.Errorf("mesh: nonce upper bytes non-zero")
		}
	}
	return binary.BigEndian.Uint64(b[4:]), nil
}
