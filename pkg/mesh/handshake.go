package mesh

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/flynn/noise"
)

// prologue is mixed into the Noise handshake hash on both sides
var prologue = []byte("meshop/v1")

// ErrPeerIDMismatch is returned (wrapped) when the remote peer's
// static public key does not hash to the PeerID the dialer expected
var ErrPeerIDMismatch = errors.New("mesh: remote peer id does not match expected")

// HandshakeConfig is the input to Handshake
type HandshakeConfig struct {
	// StaticKey is this peer's long-term identity
	StaticKey StaticKey
	// ExpectedPeerID is set by the dialer to the PeerID it expects on other end
	ExpectedPeerID PeerID
	// Initiator is true on the dialing side, false on the listening side
	Initiator bool
}

// Handshake runs Noise_XX_25519_ChaChaPoly_SHA256 over conn and
// returns a Link with the per-link Session attached
func Handshake(ctx context.Context, conn net.Conn, cfg HandshakeConfig, logger *slog.Logger) (*Link, error) {
	if conn == nil {
		return nil, fmt.Errorf("mesh: handshake: nil conn")
	}
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:   defaultCipherSuite(),
		Random:        rand.Reader,
		Pattern:       noise.HandshakeXX,
		Initiator:     cfg.Initiator,
		Prologue:      prologue,
		StaticKeypair: cfg.StaticKey.dh,
	})
	if err != nil {
		return nil, fmt.Errorf("mesh: handshake init: %w", err)
	}
	// Build a Link with no remoteID/session yet
	link := NewLink(cfg.StaticKey.PeerID(), "", conn, nil, logger)
	var sendCS, recvCS *noise.CipherState
	if cfg.Initiator {
		sendCS, recvCS, err = runInitiator(ctx, link, hs)
	} else {
		sendCS, recvCS, err = runResponder(ctx, link, hs)
	}
	if err != nil {
		return nil, err
	}

	remotePub := hs.PeerStatic()
	if len(remotePub) != keySize {
		return nil, fmt.Errorf("mesh: handshake: remote static key has unexpected length: %d", len(remotePub))
	}
	remoteID := peerIDFromPublicKey(remotePub)

	if cfg.ExpectedPeerID != "" && remoteID != cfg.ExpectedPeerID {
		return nil, fmt.Errorf("mesh: handshake: %w (expected %s, got %s)",
			ErrPeerIDMismatch, cfg.ExpectedPeerID, remoteID)
	}
	link.remoteID = remoteID
	link.session = &Session{
		sendCS:   sendCS,
		recvCS:   recvCS,
		remoteID: remoteID,
	}
	return link, nil
}

// runInitiator drives the dialer side of Noise XX
// After 3 msg both sides hold a pair of CipherStates.
func runInitiator(ctx context.Context, l *Link, hs *noise.HandshakeState) (send, recv *noise.CipherState, err error) {
	out1, _, _, err := hs.WriteMessage(nil, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg1 build: %w", err)
	}
	if err := l.writeFrame(ctx, out1); err != nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg1 send: %w", err)
	}

	in2, err := l.readFrame(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg2 recv: %w", err)
	}
	if _, _, _, err := hs.ReadMessage(nil, in2); err != nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg2 parse: %w", err)
	}

	out3, cs1, cs2, err := hs.WriteMessage(nil, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg3 build: %w", err)
	}
	if cs1 == nil || cs2 == nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg3: cipher states not produced")
	}
	if err := l.writeFrame(ctx, out3); err != nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg3 send: %w", err)
	}
	return cs1, cs2, nil
}

// runResponder drives the listening side of Noise XX
func runResponder(ctx context.Context, l *Link, hs *noise.HandshakeState) (send, recv *noise.CipherState, err error) {
	in1, err := l.readFrame(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg1 recv: %w", err)
	}
	if _, _, _, err := hs.ReadMessage(nil, in1); err != nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg1 parse: %w", err)
	}

	out2, _, _, err := hs.WriteMessage(nil, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg2 build: %w", err)
	}
	if err := l.writeFrame(ctx, out2); err != nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg2 send: %w", err)
	}

	in3, err := l.readFrame(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg3 recv: %w", err)
	}
	out, cs1, cs2, err := hs.ReadMessage(nil, in3)
	if err != nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg3 parse: %w", err)
	}
	if len(out) > 0 {
		return nil, nil, fmt.Errorf("mesh: handshake msg3: unexpected payload (%d bytes)", len(out))
	}
	if cs1 == nil || cs2 == nil {
		return nil, nil, fmt.Errorf("mesh: handshake msg3: cipher states not produced")
	}
	return cs2, cs1, nil
}
