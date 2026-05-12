package mesh

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// sessionPair holds one side of a handshaken pair: a Link and its Session
type sessionPair struct {
	id   PeerID
	link *Link
	sess *Session
}

// handshakePair returns two sides of a Noise XX handshake over a real loopback TCP pair
func handshakePair(t *testing.T) (alice, bob sessionPair) {
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
		link *Link
		err  error
	}
	cdone := make(chan result, 1)
	sdone := make(chan result, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		l, err := Handshake(ctx, clientConn, HandshakeConfig{
			StaticKey:      clientKey,
			ExpectedPeerID: serverKey.PeerID(),
			Initiator:      true,
		}, nil)
		cdone <- result{l, err}
	}()
	go func() {
		l, err := Handshake(ctx, serverConn, HandshakeConfig{
			StaticKey: serverKey,
			Initiator: false,
		}, nil)
		sdone <- result{l, err}
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
		_ = cr.link.Close()
		_ = sr.link.Close()
	})
	return sessionPair{id: clientKey.PeerID(), link: cr.link, sess: cr.link.Session()},
		sessionPair{id: serverKey.PeerID(), link: sr.link, sess: sr.link.Session()}
}

// sendChat encrypts env with sender's session and writes it on sender's link
func sendChat(ctx context.Context, sp sessionPair, env Envelope) error {
	enc, err := sp.sess.Encrypt(env)
	if err != nil {
		return err
	}
	return sp.link.SendEnvelope(ctx, enc)
}

// recvChat reads the next frame from receiver's link and decrypts it
func recvChat(ctx context.Context, sp sessionPair) (Envelope, error) {
	env, err := sp.link.ReceiveEnvelope(ctx)
	if err != nil {
		return Envelope{}, err
	}
	return sp.sess.Decrypt(env)
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

			env, err := NewEnvelope(alice.id, bob.id, "chat", tc.payload)
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
				sendErr = sendChat(ctx, alice, env)
			}()

			got, err := recvChat(ctx, bob)
			if err != nil {
				t.Fatalf("recvChat: %v", err)
			}
			wg.Wait()
			if sendErr != nil {
				t.Fatalf("sendChat: %v", sendErr)
			}

			if !bytes.Equal(got.Payload, tc.payload) {
				t.Errorf("Payload: got %v want %v", got.Payload, tc.payload)
			}
			if got.From != alice.id {
				t.Errorf("From: got %q want %q", got.From, alice.id)
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
		}, nil)
		cerr <- err
	}()
	go func() {
		_, _ = Handshake(ctx, serverConn, HandshakeConfig{
			StaticKey: serverKey,
			Initiator: false,
		}, nil)
	}()

	err := <-cerr
	if !errors.Is(err, ErrPeerIDMismatch) {
		t.Fatalf("client handshake: got %v, want ErrPeerIDMismatch", err)
	}
}

func TestSessionRejectsTamperedCiphertext(t *testing.T) {
	alice, bob := handshakePair(t)

	env, _ := NewEnvelope(alice.id, bob.id, "chat", []byte("secret"))
	env.Nonce = encodeNonce(alice.sess.sendCS.Nonce())
	aad := envelopeAAD(env)
	ct, err := alice.sess.sendCS.Encrypt(nil, aad, env.Payload)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if len(ct) == 0 {
		t.Fatal("empty ciphertext")
	}
	ct[0] ^= 0x01
	env.Payload = ct

	if err := alice.link.sendUnderLockForTest(t, env); err != nil {
		t.Fatalf("raw send: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if _, err := recvChat(ctx, bob); err == nil {
		t.Fatal("recvChat accepted tampered ciphertext")
	}
}

func TestSessionRejectsTamperedHeader(t *testing.T) {
	alice, bob := handshakePair(t)

	env, _ := NewEnvelope(alice.id, bob.id, "chat", []byte("secret"))
	env.Nonce = encodeNonce(alice.sess.sendCS.Nonce())
	aad := envelopeAAD(env)
	ct, err := alice.sess.sendCS.Encrypt(nil, aad, env.Payload)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	env.Payload = ct
	env.From = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

	if err := alice.link.sendUnderLockForTest(t, env); err != nil {
		t.Fatalf("raw send: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if _, err := recvChat(ctx, bob); err == nil {
		t.Fatal("recvChat accepted tampered header")
	}
}

func TestSessionRejectsReplay(t *testing.T) {
	alice, bob := handshakePair(t)

	env, _ := NewEnvelope(alice.id, bob.id, "chat", []byte("once"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := sendChat(ctx, alice, env); err != nil {
		t.Fatalf("first send: %v", err)
	}
	if _, err := recvChat(ctx, bob); err != nil {
		t.Fatalf("first recv: %v", err)
	}

	bad := env
	bad.Nonce = encodeNonce(0)
	bad.Payload = []byte("anything")
	if err := alice.link.sendUnderLockForTest(t, bad); err != nil {
		t.Fatalf("raw send: %v", err)
	}
	if _, err := recvChat(ctx, bob); err == nil {
		t.Fatal("recvChat accepted stale nonce")
	}
}
