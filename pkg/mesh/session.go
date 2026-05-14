package mesh

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/flynn/noise"
)

// nonceBytes is the AEAD nonce length
const nonceBytes = 12

// Session is an authenticated, encrypted channel between two peers.
// It holds cipher states only; it owns no I/O. Encrypt and Decrypt are
// safe to call from multiple goroutines.
type Session struct {
	sendCS   *noise.CipherState
	recvCS   *noise.CipherState
	remoteID PeerID
	mu       sync.Mutex
}

// RemoteID returns the PeerID of the remote peer
func (s *Session) RemoteID() PeerID { return s.remoteID }

// Encrypt sets env.Nonce from the send counter, computes AAD over the
// routing fields, encrypts env.Payload, and returns the ready-to-send
// envelope
func (s *Session) Encrypt(env Envelope) (Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.encryptLocked(env)
}

// encryptLocked is Encrypt without taking s.mu Caller must hold in current structure (might change)
func (s *Session) encryptLocked(env Envelope) (Envelope, error) {
	env.Nonce = encodeNonce(s.sendCS.Nonce())
	aad := envelopeAAD(env)
	ct, err := s.sendCS.Encrypt(nil, aad, env.Payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("mesh: session encrypt: %w", err)
	}
	env.Payload = ct
	return env, nil
}

// Decrypt verifies env.Nonce against the recv counter, computes AAD,
// and decrypts env.Payload. Returns the envelope with plaintext payload
func (s *Session) Decrypt(env Envelope) (Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(env.Nonce) != nonceBytes {
		return Envelope{}, fmt.Errorf("mesh: session decrypt: bad nonce length %d", len(env.Nonce))
	}
	got, err := decodeNonce(env.Nonce)
	if err != nil {
		return Envelope{}, fmt.Errorf("mesh: session decrypt: %w", err)
	}
	want := s.recvCS.Nonce()
	if got != want {
		return Envelope{}, fmt.Errorf("mesh: session decrypt: nonce mismatch (got %d, want %d)", got, want)
	}
	aad := envelopeAAD(env)
	pt, err := s.recvCS.Decrypt(nil, aad, env.Payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("mesh: session decrypt: %w", err)
	}
	env.Payload = pt
	return env, nil
}

// envelopeAAD returns deterministic associated-data bytes for env.
// The Payload field is excluded (it is the AEAD plaintext). TTL is
// excluded too (it changes at every hop). Every other field is bound
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
