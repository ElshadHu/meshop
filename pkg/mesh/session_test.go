package mesh

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// handshakePair returns two Sessions over a real loopback TCP pair.
func handshakePair(t *testing.T) (initiator, responder *Session) {
	t.Helper()

	clientConn, serverConn := dialPair(t)

	clientKey, err := GenerateKey()
	if err != nil {
		t.Fatalf("client key: %v", err)
	}
	serverKey, err := GenerateKey()
	if err != nil {
		t.Fatalf("server key: %v", err)
	}

	type result struct {
		s   *Session
		err error
	}
	cdone := make(chan result, 1)
	sdone := make(chan result, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		s, err := Handshake(ctx, clientConn, HandshakeConfig{
			StaticKey:      clientKey,
			ExpectedPeerID: serverKey.PeerID(),
			Initiator:      true,
		})
		cdone <- result{s, err}
	}()
	go func() {
		s, err := Handshake(ctx, serverConn, HandshakeConfig{
			StaticKey: serverKey,
			Initiator: false,
		})
		sdone <- result{s, err}
	}()

	cr := <-cdone
	if cr.err != nil {
		t.Fatalf("client handshake: %v", cr.err)
	}
	sr := <-sdone
	if sr.err != nil {
		t.Fatalf("server handshake: %v", sr.err)
	}

	t.Cleanup(func() {
		_ = cr.s.Close()
		_ = sr.s.Close()
	})
	return cr.s, sr.s
}

func TestSessionRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"small text", []byte("hello, bob")},
		{"empty payload", []byte{}},
		{"binary with nulls", []byte{0x00, 0x01, 0x02, 0xFF, 0x00}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			alice, bob := handshakePair(t)

			env, err := NewEnvelope(alice.LocalID(), bob.LocalID(), "chat", tc.payload)
			if err != nil {
				t.Fatalf("NewEnvelope: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			var wg sync.WaitGroup
			wg.Add(1)
			var sendErr error
			go func() {
				defer wg.Done()
				sendErr = alice.Send(ctx, env)
			}()

			got, err := bob.Recv(ctx)
			if err != nil {
				t.Fatalf("bob.Recv: %v", err)
			}
			wg.Wait()
			if sendErr != nil {
				t.Fatalf("alice.Send: %v", sendErr)
			}

			if !bytes.Equal(got.Payload, tc.payload) {
				t.Errorf("Payload: got %v want %v", got.Payload, tc.payload)
			}
			if got.From != alice.LocalID() {
				t.Errorf("From: got %q want %q", got.From, alice.LocalID())
			}
			if len(got.Nonce) != nonceBytes {
				t.Errorf("Nonce length: got %d want %d", len(got.Nonce), nonceBytes)
			}
		})
	}
}

func TestHandshakeRejectsWrongPeerID(t *testing.T) {
	clientConn, serverConn := dialPair(t)

	clientKey, _ := GenerateKey()
	serverKey, _ := GenerateKey()
	wrongKey, _ := GenerateKey()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cerr := make(chan error, 1)
	go func() {
		_, err := Handshake(ctx, clientConn, HandshakeConfig{
			StaticKey:      clientKey,
			ExpectedPeerID: wrongKey.PeerID(),
			Initiator:      true,
		})
		cerr <- err
	}()
	go func() {
		_, _ = Handshake(ctx, serverConn, HandshakeConfig{
			StaticKey: serverKey,
			Initiator: false,
		})
	}()

	err := <-cerr
	if !errors.Is(err, ErrPeerIDMismatch) {
		t.Fatalf("client handshake: got %v, want ErrPeerIDMismatch", err)
	}
}

func TestSessionRejectsTamperedCiphertext(t *testing.T) {
	alice, bob := handshakePair(t)

	env, _ := NewEnvelope(alice.LocalID(), bob.LocalID(), "chat", []byte("secret"))
	env.Nonce = encodeNonce(alice.sendCS.Nonce())
	aad := envelopeAAD(env)
	ct, err := alice.sendCS.Encrypt(nil, aad, env.Payload)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if len(ct) == 0 {
		t.Fatal("empty ciphertext")
	}
	ct[0] ^= 0x01
	env.Payload = ct

	if err := alice.node.sendUnderLockForTest(t, env); err != nil {
		t.Fatalf("raw send: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if _, err := bob.Recv(ctx); err == nil {
		t.Fatal("Recv accepted tampered ciphertext")
	}
}

func TestSessionRejectsTamperedHeader(t *testing.T) {
	alice, bob := handshakePair(t)

	env, _ := NewEnvelope(alice.LocalID(), bob.LocalID(), "chat", []byte("secret"))
	env.Nonce = encodeNonce(alice.sendCS.Nonce())
	aad := envelopeAAD(env)
	ct, err := alice.sendCS.Encrypt(nil, aad, env.Payload)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	env.Payload = ct
	env.From = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

	if err := alice.node.sendUnderLockForTest(t, env); err != nil {
		t.Fatalf("raw send: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if _, err := bob.Recv(ctx); err == nil {
		t.Fatal("Recv accepted tampered header")
	}
}

func TestSessionRejectsReplay(t *testing.T) {
	alice, bob := handshakePair(t)

	env, _ := NewEnvelope(alice.LocalID(), bob.LocalID(), "chat", []byte("once"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := alice.Send(ctx, env); err != nil {
		t.Fatalf("first send: %v", err)
	}
	if _, err := bob.Recv(ctx); err != nil {
		t.Fatalf("first recv: %v", err)
	}

	bad := env
	bad.Nonce = encodeNonce(0)
	bad.Payload = []byte("anything")
	if err := alice.node.sendUnderLockForTest(t, bad); err != nil {
		t.Fatalf("raw send: %v", err)
	}
	if _, err := bob.Recv(ctx); err == nil {
		t.Fatal("Recv accepted stale nonce")
	}
}

