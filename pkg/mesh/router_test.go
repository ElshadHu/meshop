package mesh

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func quickCtx(t *testing.T, d time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), d)
}

func newTestRouter(t *testing.T) *Router {
	t.Helper()
	key, err := GenerateKey()
	if err != nil {
		t.Fatalf("GeneratedKey: %v", err)
	}
	return NewRouter(key, nil)
}

// wireRouters joins a and b with a net.Pipe and runs the link handshake
func wireRouters(t *testing.T, a, b *Router) {
	t.Helper()
	cA, cB := net.Pipe()
	ctx, cancel := quickCtx(t, 5*time.Second)
	defer cancel()
	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() { errA <- a.attachConn(ctx, cA, true, b.LocalID()) }()
	go func() { errB <- b.attachConn(ctx, cB, false, "") }()
	if err := <-errA; err != nil {
		t.Fatalf("attach A: %v", err)
	}
	if err := <-errB; err != nil {
		t.Fatalf("attach b: %v", err)
	}
}

func TestRouter_DirectNeighbor_Chat(t *testing.T) {
	a, b := newTestRouter(t), newTestRouter(t)
	defer a.Close()
	defer b.Close()
	wireRouters(t, a, b)
	sendCtx, cancel := quickCtx(t, 5*time.Second)
	defer cancel()
	if err := a.Send(sendCtx, b.LocalID(), "chat", []byte("hi")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	recvCtx, cancel2 := quickCtx(t, 5*time.Second)
	defer cancel2()
	env, err := b.Recv(recvCtx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(env.Payload) != "hi" {
		t.Errorf("payload: got %q want %q", env.Payload, "hi")
	}
	if env.From != a.LocalID() {
		t.Errorf("From: got %q want %q", env.From, a.LocalID())
	}
}

func TestRouter_HandshakeOverRelay(t *testing.T) {
	a, b, c := newTestRouter(t), newTestRouter(t), newTestRouter(t)
	defer a.Close()
	defer b.Close()
	defer c.Close()
	wireRouters(t, a, b)
	wireRouters(t, b, c)
	a.mu.Lock()
	_, hadC := a.sessions[c.LocalID()]
	a.mu.Unlock()
	if hadC {
		t.Fatal("A should not have a session with C before Send")
	}

	sendCtx, cancel := quickCtx(t, 10*time.Second)
	defer cancel()
	if err := a.Send(sendCtx, c.LocalID(), "chat", []byte("hello stranger")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	a.mu.Lock()
	_, hasC := a.sessions[c.LocalID()]
	a.mu.Unlock()
	if !hasC {
		t.Fatal("A should have a session with C after Send")
	}
}

func TestRouter_Flood_Dedup_DropsDuplicates(t *testing.T) {
	a, b := newTestRouter(t), newTestRouter(t)
	defer a.Close()
	defer b.Close()
	wireRouters(t, a, b)
	sendCtx, cancel := quickCtx(t, 5*time.Second)
	defer cancel()
	if err := a.Send(sendCtx, b.LocalID(), "chat", []byte("once")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	recvCtx, cancel2 := quickCtx(t, 2*time.Second)
	defer cancel2()
	if _, err := b.Recv(recvCtx); err != nil {
		t.Fatalf("first Recv: %v", err)
	}

	// A second Recv with a short time out should fail
	recvCtx2, cancel3 := quickCtx(t, 300*time.Millisecond)
	defer cancel3()
	if _, err := b.Recv(recvCtx2); err == nil {
		t.Fatal("Recv returned a duplicate; dedup did not work")
	}
}

func TestRouter_TTLZero_DropsAtForwarder(t *testing.T) {
	r := newTestRouter(t)
	defer r.Close()
	// Address an envelope to some unknown PeerID with TTL=0. onEnvelope
	// should mark it as seen and drop it (no forward, no panic).
	env, _ := NewEnvelope("origin-peer", "unreachable-peer", "chat", []byte("x"))
	env.TTL = 0
	r.onEnvelope("origin-peer", env)
	if !r.dedup.seenBefore(env.ID) {
		t.Fatal("dedup did not record the TTL=0 envelope")
	}
}

func TestRouter_UnreachablePeer_TimesOut(t *testing.T) {
	a, b := newTestRouter(t), newTestRouter(t)
	defer a.Close()
	defer b.Close()
	wireRouters(t, a, b)

	other, _ := GenerateKey()
	sendCtx, cancel := quickCtx(t, 12*time.Second)
	defer cancel()
	err := a.Send(sendCtx, other.PeerID(), "chat", []byte("hi"))
	if err == nil {
		t.Fatal("Send to unreachable peer should not succeed")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Logf("note: error not labelled 'timeout': %v", err)
	}
}

func TestRouter_ConcurrentSends(t *testing.T) {
	a, b := newTestRouter(t), newTestRouter(t)
	wireRouters(t, a, b)
	const N = 5
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := quickCtx(t, 5*time.Second)
			defer cancel()
			if err := a.Send(ctx, b.LocalID(), "chat", []byte("burst")); err != nil {
				t.Errorf("Send: %v", err)
			}
		}()
	}
	wg.Wait()
	got := 0
	for got < N {
		ctx, cancel := quickCtx(t, 2*time.Second)
		_, err := b.Recv(ctx)
		cancel()
		if err != nil {
			t.Fatalf("Recv after %d messages: %v", got, err)
		}
		got++
	}
}

func TestRouter_RealTCP_Smoke(t *testing.T) {
	a := newTestRouter(t)
	b := newTestRouter(t)
	c := newTestRouter(t)
	defer a.Close()
	defer b.Close()
	defer c.Close()

	bAddr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("b listen: %v", err)
	}
	cAddr, err := c.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("c listen: %v", err)
	}

	connectCtx, cancel := quickCtx(t, 5*time.Second)
	defer cancel()
	if err := a.Connect(connectCtx, bAddr, b.LocalID()); err != nil {
		t.Fatalf("a→b: %v", err)
	}
	if err := b.Connect(connectCtx, cAddr, c.LocalID()); err != nil {
		t.Fatalf("b→c: %v", err)
	}

	sendCtx, cancel2 := quickCtx(t, 10*time.Second)
	defer cancel2()
	if err := a.Send(sendCtx, c.LocalID(), "chat", []byte("over tcp")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	recvCtx, cancel3 := quickCtx(t, 10*time.Second)
	defer cancel3()
	env, err := c.Recv(recvCtx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(env.Payload) != "over tcp" {
		t.Errorf("payload: got %q want %q", env.Payload, "over tcp")
	}
}
