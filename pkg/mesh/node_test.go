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

// sendUnderLockForTest writes env bypassing Session.Send's encryption.
func (n *Node) sendUnderLockForTest(t testing.TB, env Envelope) error {
	t.Helper()
	n.sendMu.Lock()
	defer n.sendMu.Unlock()
	return n.sendUnderLock(context.Background(), env)
}

func dialPair(t *testing.T) (client, server net.Conn) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	type acceptResult struct {
		conn net.Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		c, err := l.Accept()
		accepted <- acceptResult{c, err}
	}()

	dialer := net.Dialer{Timeout: 2 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err = dialer.DialContext(ctx, "tcp", l.Addr().String())
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

// TestNodeRoundTripTCP sends an Envelope from one Node to another over a
// real loopback TCP connection and verifies it arrives intact
func TestNodeRoundTripTCP(t *testing.T) {
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

			alice := NewNode(aID, aConn)
			bob := NewNode(bID, bConn)

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
				sendErr = alice.Send(ctx, sent)
			}()

			got, err := bob.Recv(ctx)
			if err != nil {
				t.Fatalf("bob.Recv: %v", err)
			}
			wg.Wait()
			if sendErr != nil {
				t.Fatalf("alice.Send: %v", sendErr)
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

// TestRecvCancelByContext verifies that a Recv call blocked on the
// network unblocks when its context is cancelled, and the returned
// error wraps context.Canceled.
func TestRecvCancelByContext(t *testing.T) {
	aConn, _ := dialPair(t)

	aID := testPeerID(t)
	alice := NewNode(aID, aConn)

	ctx, cancel := context.WithCancel(context.Background())

	type recvResult struct {
		env Envelope
		err error
	}
	done := make(chan recvResult, 1)
	go func() {
		env, err := alice.Recv(ctx)
		done <- recvResult{env, err}
	}()

	// Give Recv time to actually park on the read. (I might change later)
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case r := <-done:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("Recv after cancel: got %v, want context.Canceled wrapped", r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Recv did not return within 1s of ctx cancellation")
	}
}

// TestRecvReturnsEOFOnPeerClose verifies that Recv returns io.EOF when
// the peer closes its side of the TCP connection at a frame boundary.
func TestRecvReturnsEOFOnPeerClose(t *testing.T) {
	aConn, bConn := dialPair(t)

	aID := testPeerID(t)
	bID := testPeerID(t)

	alice := NewNode(aID, aConn)
	bob := NewNode(bID, bConn)

	if err := alice.Close(); err != nil {
		t.Fatalf("alice.Close: %v", err)
	}

	_, err := bob.Recv(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("bob.Recv after peer close: got %v, want io.EOF", err)
	}
}
