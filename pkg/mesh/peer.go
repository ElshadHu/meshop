// Package mesh implements peer-to-peer mesh messaging primitives.
package mesh

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/flynn/noise"
)

// PeerID identifies a peer in the network
type PeerID string

// keySize is the byte length of one half (private or public) of a static Key
const keySize = 32

// peerIDFromPublicKey returns the PeerID derived from a peer's public key
func peerIDFromPublicKey(pub []byte) PeerID {
	sum := sha256.Sum256(pub)
	return PeerID(hex.EncodeToString(sum[:]))
}

// StaticKey is the long-term identity material for one peer
type StaticKey struct {
	dh noise.DHKey
}

// GenerateKey Uses crypto/rand  for security
func GenerateKey() (StaticKey, error) {
	cs := defaultCipherSuite()
	dh, err := cs.GenerateKeypair(rand.Reader)
	if err != nil {
		return StaticKey{}, fmt.Errorf("mesh: generate key: %w", err)
	}
	return StaticKey{dh: dh}, nil
}

// PeerID returns the PeerID derived from this key's public half
func (k StaticKey) PeerID() PeerID {
	return peerIDFromPublicKey(k.dh.Public)
}

// PublicKey returns a copy of the public-key bytes
func (k StaticKey) PublicKey() []byte {
	out := make([]byte, len(k.dh.Public))
	copy(out, k.dh.Public)
	return out
}

// MarshalBinary returns the 64-byte serialised form: 32 bytes private
func (k StaticKey) MarshalBinary() ([]byte, error) {
	if len(k.dh.Private) != keySize || len(k.dh.Public) != keySize {
		return nil, fmt.Errorf("mesh: marshal key: malformed key")
	}

	out := make([]byte, 0, 2*keySize)
	out = append(out, k.dh.Private...)
	out = append(out, k.dh.Public...)
	return out, nil
}

// UnmarshalBinary loads a StaticKey from bytes produced by MarshalBinary
func (k *StaticKey) UnmarshalBinary(b []byte) error {
	if len(b) != 2*keySize {
		return fmt.Errorf("mesh: unmarshal key: want %d bytes, got %d", 2*keySize, len(b))
	}

	priv := make([]byte, keySize)
	pub := make([]byte, keySize)
	copy(priv, b[:keySize])
	copy(pub, b[keySize:])
	k.dh = noise.DHKey{Private: priv, Public: pub}
	return nil
}

// defaultCipherSuite is the single locked-in Noise cipher
func defaultCipherSuite() noise.CipherSuite {
	return noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashSHA256)
}
