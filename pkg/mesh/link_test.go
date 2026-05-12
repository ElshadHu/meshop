package mesh

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func testPeerID(t *testing.T) PeerID {
	t.Helper()
	k, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return k.PeerID()
}

// sendUnderLockForTest writes env bypassing Session encryption
func (l *Link) sendUnderLockForTest(t testing.TB, env Envelope) error {
	t.Helper()
	return l.SendEnvelope(context.Background(), env)
}

func dialPair(t *testing.T) (client, server net.Conn) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	type acceptResult struct {
		conn net.Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		c, err := ln.Accept()
		accepted <- acceptResult{c, err}
	}()

	dialer := net.Dialer{Timeout: 2 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err = dialer.DialContext(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ar := <-accepted
	if ar.err != nil {
		t.Fatalf("accept: %v", ar.err)
	}
	t.Cleanup(func() { _ = ar.conn.Close() })

	return client, ar.conn
}

// TestLinkRoundTripTCP sends an Envelope from one Link to another over a
// real loopback TCP connection and verifies it arrives intact
func TestLinkRoundTripTCP(t *testing.T) {
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
			aConn, bConn := dialPair(t)

			aID := testPeerID(t)
			bID := testPeerID(t)

			alice := NewLink(aID, bID, aConn, nil, nil)
			bob := NewLink(bID, aID, bConn, nil, nil)

			sent, err := NewEnvelope(aID, bID, "chat", tc.payload)
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
				sendErr = alice.SendEnvelope(ctx, sent)
			}()

			got, err := bob.ReceiveEnvelope(ctx)
			if err != nil {
				t.Fatalf("bob.ReceiveEnvelope: %v", err)
			}
			wg.Wait()
			if sendErr != nil {
				t.Fatalf("alice.SendEnvelope: %v", sendErr)
			}

			if got.ID != sent.ID {
				t.Errorf("ID:        got %q want %q", got.ID, sent.ID)
			}
			if got.From != sent.From {
				t.Errorf("From:      got %q want %q", got.From, sent.From)
			}
			if got.To != sent.To {
				t.Errorf("To:        got %q want %q", got.To, sent.To)
			}
			if got.Type != sent.Type {
				t.Errorf("Type:      got %q want %q", got.Type, sent.Type)
			}
			if !got.Timestamp.Equal(sent.Timestamp) {
				t.Errorf("Timestamp: got %v want %v", got.Timestamp, sent.Timestamp)
			}
			if !bytes.Equal(got.Payload, sent.Payload) {
				t.Errorf("Payload:   got %v want %v", got.Payload, sent.Payload)
			}
		})
	}
}

// TestReceiveCancelByContext verifies that a ReceiveEnvelope call blocked
// on the network unblocks when its context is cancelled
func TestReceiveCancelByContext(t *testing.T) {
	aConn, _ := dialPair(t)

	aID := testPeerID(t)
	alice := NewLink(aID, "", aConn, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())

	type recvResult struct {
		env Envelope
		err error
	}
	done := make(chan recvResult, 1)
	go func() {
		env, err := alice.ReceiveEnvelope(ctx)
		done <- recvResult{env, err}
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case r := <-done:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("ReceiveEnvelope after cancel: got %v, want context.Canceled wrapped", r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("ReceiveEnvelope did not return within 1s of ctx cancellation")
	}
}

// TestReceiveReturnsEOFOnPeerClose verifies that ReceiveEnvelope returns
// io.EOF when the peer closes its side of the TCP connection
func TestReceiveReturnsEOFOnPeerClose(t *testing.T) {
	aConn, bConn := dialPair(t)

	aID := testPeerID(t)
	bID := testPeerID(t)

	alice := NewLink(aID, bID, aConn, nil, nil)
	bob := NewLink(bID, aID, bConn, nil, nil)

	if err := alice.Close(); err != nil {
		t.Fatalf("alice.Close: %v", err)
	}

	_, err := bob.ReceiveEnvelope(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("bob.ReceiveEnvelope after peer close: got %v, want io.EOF", err)
	}
}
