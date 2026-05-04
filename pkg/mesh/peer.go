// Package mesh implement peer-to-peer mesh messaging primitives

package mesh

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// PeerID identifies a peer in the network
// later I can do it SHA256 public key
type PeerID string

// peerIDBytes is the number of random bytes
const peerIDBytes = 16

// NewPeerID returns a  generated random PeerID

func NewPeerID() (PeerID, error) {
	b := make([]byte, peerIDBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("mesh generated peer id: %w", err)
	}
	return PeerID(hex.EncodeToString(b)), nil
}
