// Command demo runs Goal 3 of meshop: two computers chat over a
// Noise-XX-encrypted channel on top of TCP. One side runs with
// --listen :PORT, the other with --dial host:PORT --peer PEERID
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ElshadHu/meshop/pkg/mesh"
)

const (
	dialTimeout      = 10 * time.Second
	keepAlivePeriod  = 30 * time.Second
	handshakeTimeout = 10 * time.Second
	sendTimeout      = 5 * time.Second
	reconnectMinWait = 1 * time.Second
	reconnectMaxWait = 30 * time.Second
	keyFileMode      = 0o600
	keyFileDirMode   = 0o700
)

func main() {
	listenAddr := flag.String("listen", "", "address to listen on, e.g. :9000")
	dialAddr := flag.String("dial", "", "address to dial, e.g. 192.168.1.42:9000")
	peerID := flag.String("peer", "", "expected remote PeerID (required with --dial)")
	keyPath := flag.String("key", defaultKeyPath(), "path to static identity key file")
	flag.Parse()

	if (*listenAddr == "") == (*dialAddr == "") {
		log.Fatal("specify exactly one of --listen or --dial")
	}
	if *dialAddr != "" && *peerID == "" {
		log.Fatal("--dial requires --peer <PeerID>")
	}

	key, fresh, err := loadOrCreateKey(*keyPath)
	if err != nil {
		log.Fatalf("key: %v", err)
	}
	if fresh {
		fmt.Printf("generated new identity at %s\n", *keyPath)
	}
	fmt.Printf("local peer id: %s\n", key.PeerID())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *listenAddr != "" {
		runListen(ctx, *listenAddr, key)
	} else {
		runDial(ctx, *dialAddr, mesh.PeerID(*peerID), key)
	}
}

func defaultKeyPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "meshop.key"
	}
	return filepath.Join(home, ".meshop", "key")
}

// loadOrCreateKey reads the StaticKey at path, generating and saving
// a new one if the file does not exist.
func loadOrCreateKey(path string) (mesh.StaticKey, bool, error) {
	b, err := os.ReadFile(path)
	if err == nil {
		var k mesh.StaticKey
		if err := k.UnmarshalBinary(b); err != nil {
			return mesh.StaticKey{}, false, fmt.Errorf("decode %s: %w", path, err)
		}
		return k, false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return mesh.StaticKey{}, false, fmt.Errorf("read %s: %w", path, err)
	}

	if err := os.MkdirAll(filepath.Dir(path), keyFileDirMode); err != nil {
		return mesh.StaticKey{}, false, fmt.Errorf("mkdir: %w", err)
	}
	k, err := mesh.GenerateKey()
	if err != nil {
		return mesh.StaticKey{}, false, fmt.Errorf("generate: %w", err)
	}
	out, err := k.MarshalBinary()
	if err != nil {
		return mesh.StaticKey{}, false, fmt.Errorf("marshal: %w", err)
	}
	if err := os.WriteFile(path, out, keyFileMode); err != nil {
		return mesh.StaticKey{}, false, fmt.Errorf("write %s: %w", path, err)
	}
	return k, true, nil
}

// runListen accepts one connection at a time. After a session ends it
// goes back to Accept.
func runListen(ctx context.Context, addr string, key mesh.StaticKey) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}
	defer func() { _ = l.Close() }()
	fmt.Printf("listening on %s\n", l.Addr())

	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Accept: %v", err)
			return
		}
		fmt.Printf("\npeer connected from %s\n", conn.RemoteAddr())
		configureTCP(conn)
		runSession(ctx, conn, key, "")
		if ctx.Err() != nil {
			return
		}
		fmt.Println("waiting for next peer...")
	}
}

// runDial dials in a loop with exponential backoff, then runs one
// authenticated session.
func runDial(ctx context.Context, addr string, expected mesh.PeerID, key mesh.StaticKey) {
	wait := reconnectMinWait
	for ctx.Err() == nil {
		dialer := &net.Dialer{Timeout: dialTimeout}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			log.Printf("dial %s: %v (retry in %s)", addr, err, wait)
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return
			}
			if wait < reconnectMaxWait {
				wait *= 2
				if wait > reconnectMaxWait {
					wait = reconnectMaxWait
				}
			}
			continue
		}
		wait = reconnectMinWait
		fmt.Printf("\nconnected to %s\n", conn.RemoteAddr())
		configureTCP(conn)
		runSession(ctx, conn, key, expected)
	}
}

func configureTCP(c net.Conn) {
	t, ok := c.(*net.TCPConn)
	if !ok {
		return
	}
	if err := t.SetKeepAlive(true); err != nil {
		log.Printf("keepalive: %v", err)
	}
	if err := t.SetKeepAlivePeriod(keepAlivePeriod); err != nil {
		log.Printf("keepalive period: %v", err)
	}
}

// runSession runs the Noise handshake then a chat loop on top of the
// resulting Session. expected is "" on the listener side.
func runSession(ctx context.Context, conn net.Conn, key mesh.StaticKey, expected mesh.PeerID) {
	hctx, hcancel := context.WithTimeout(ctx, handshakeTimeout)
	session, err := mesh.Handshake(hctx, conn, mesh.HandshakeConfig{
		StaticKey:      key,
		ExpectedPeerID: expected,
		Initiator:      expected != "",
	})
	hcancel()
	if err != nil {
		log.Printf("handshake: %v", err)
		_ = conn.Close()
		return
	}
	defer func() { _ = session.Close() }()

	fmt.Printf("authenticated peer id: %s\n", session.RemoteID())
	fmt.Print("> ")

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	recvErr := make(chan error, 1)
	go func() {
		for {
			env, err := session.Recv(sessionCtx)
			if err != nil {
				recvErr <- err
				return
			}
			fmt.Printf("\r<%s> %s\n> ", short(env.From), env.Payload)
		}
	}()

	sendErr := make(chan error, 1)
	go func() {
		sendErr <- runStdinSend(sessionCtx, session)
	}()

	select {
	case err := <-recvErr:
		if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
			fmt.Println("\npeer disconnected")
		} else {
			log.Printf("\nrecv: %v", err)
		}
	case err := <-sendErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("send: %v", err)
		}
	case <-sessionCtx.Done():
	}
}

// runStdinSend reads lines from stdin and sends each as a chat envelope
func runStdinSend(ctx context.Context, s *mesh.Session) error {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		text := scanner.Text()
		if text == "" {
			fmt.Print("> ")
			continue
		}
		env, err := mesh.NewEnvelope(s.LocalID(), s.RemoteID(), "chat", []byte(text))
		if err != nil {
			return fmt.Errorf("build envelope: %w", err)
		}
		sctx, scancel := context.WithTimeout(ctx, sendTimeout)
		err = s.Send(sctx, env)
		scancel()
		if err != nil {
			return fmt.Errorf("send: %w", err)
		}
		fmt.Print("> ")
	}
	return scanner.Err()
}

// short returns the first 8 chars of a PeerID, for compact display
func short(id mesh.PeerID) string {
	if len(id) <= 8 {
		return string(id)
	}
	return string(id[:8])
}
