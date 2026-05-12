// Command demo runs Goal 3 of meshop: two computers chat over a
// Noise-XX-encrypted channel on top of TCP. One side runs with
// --listen :PORT, the other with --dial host:PORT --peer PEERID.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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

// App owns the cross-cutting state of one running demo peer.
type App struct {
	key    mesh.StaticKey
	logger *slog.Logger
}

func main() {
	listenAddr := flag.String("listen", "", "address to listen on, e.g. :9000")
	dialAddr := flag.String("dial", "", "address to dial, e.g. 192.168.1.42:9000")
	peerID := flag.String("peer", "", "expected remote PeerID (required with --dial)")
	keyPath := flag.String("key", defaultKeyPath(), "path to static identity key file")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	flag.CommandLine.SetOutput(os.Stderr)
	flag.Parse()

	if (*listenAddr == "") == (*dialAddr == "") {
		usage("specify exactly one of --listen or --dial")
	}
	if *dialAddr != "" && *peerID == "" {
		usage("--dial requires --peer <PeerID>")
	}

	if *listenAddr != "" && *peerID != "" {
		usage("--peer is only valid with --dial")
	}

	level, err := parseLogLevel(*logLevel)
	if err != nil {
		usage(err.Error())
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	key, fresh, err := loadOrCreateKey(*keyPath)
	if err != nil {
		logger.Error("key load failed", "path", *keyPath, "err", err)
		os.Exit(1)
	}
	if fresh {
		logger.Info("identity created", "path", *keyPath)
	}
	logger.Info("local peer id", "id", key.PeerID())

	app := &App{key: key, logger: logger}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *listenAddr != "" {
		app.listen(ctx, *listenAddr)
	} else {
		app.dial(ctx, *dialAddr, mesh.PeerID(*peerID))
	}
}

// usage writes a short error and the flag usage to stderr and exits 2.
// Used for "you invoked the binary wrong" cases, before the slog logger
// exists.
func usage(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	flag.Usage()
	os.Exit(2)
}

// parseLogLevel turns "debug"/"info"/"warn"/"error" into a slog.Level.
func parseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown --log-level %q", s)
	}
}

func defaultKeyPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "meshop.key"
	}
	return filepath.Join(home, ".meshop", "key")
}

// loadOrCreateKey reads the StaticKey at path, generating and saving a
// new one if the file does not exist.
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

// listen accepts one connection at a time. After a session ends it goes
// back to Accept.
func (a *App) listen(ctx context.Context, addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		a.logger.Error("listen failed", "addr", addr, "err", err)
		os.Exit(1)
	}
	defer func() { _ = l.Close() }()
	a.logger.Info("listening", "addr", l.Addr().String())

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
			a.logger.Warn("accept failed", "err", err)
			return
		}
		a.logger.Info("peer connected", "remote", conn.RemoteAddr().String())
		configureTCP(conn, a.logger)
		a.runSession(ctx, conn, "")
		if ctx.Err() != nil {
			return
		}
		a.logger.Info("waiting for next peer")
	}
}

// dial dials in a loop with exponential backoff, then runs one
// authenticated session.
func (a *App) dial(ctx context.Context, addr string, expected mesh.PeerID) {
	wait := reconnectMinWait
	for ctx.Err() == nil {
		dialer := &net.Dialer{Timeout: dialTimeout}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			a.logger.Warn("dial failed, retrying", "addr", addr, "err", err, "retry_in", wait)
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
		a.logger.Info("connected", "remote", conn.RemoteAddr().String())
		configureTCP(conn, a.logger)
		a.runSession(ctx, conn, expected)
	}
}

// configureTCP enables TCP keepalive on conn. Failures are logged and
// ignored; the session still runs without keepalive
func configureTCP(c net.Conn, logger *slog.Logger) {
	t, ok := c.(*net.TCPConn)
	if !ok {
		return
	}
	if err := t.SetKeepAlive(true); err != nil {
		logger.Warn("keepalive set failed", "err", err)
	}
	if err := t.SetKeepAlivePeriod(keepAlivePeriod); err != nil {
		logger.Warn("keepalive period set failed", "err", err)
	}
}

// runSession runs the Noise handshake then a chat loop on top of the
// resulting Session. expected is "" on the listener side.
func (a *App) runSession(ctx context.Context, conn net.Conn, expected mesh.PeerID) {
	hctx, hcancel := context.WithTimeout(ctx, handshakeTimeout)
	link, err := mesh.Handshake(hctx, conn, mesh.HandshakeConfig{
		StaticKey:      a.key,
		ExpectedPeerID: expected,
		Initiator:      expected != "",
	}, a.logger)
	hcancel()
	if err != nil {
		a.logger.Error("handshake failed", "err", err)
		_ = conn.Close()
		return
	}
	defer func() { _ = link.Close() }()

	session := link.Session()
	sessLog := a.logger.With("remote", session.RemoteID())
	sessLog.Info("authenticated")
	fmt.Print("> ")

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	recvErr := make(chan error, 1)
	go func() {
		for {
			env, err := link.ReceiveEnvelope(sessionCtx)
			if err != nil {
				recvErr <- err
				return
			}
			plain, err := session.Decrypt(env)
			if err != nil {
				recvErr <- err
				return
			}
			fmt.Printf("\r<%s> %s\n> ", short(plain.From), plain.Payload)
		}
	}()

	sendErr := make(chan error, 1)
	go func() {
		sendErr <- a.runStdinSend(sessionCtx, link)
	}()

	select {
	case err := <-recvErr:
		if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
			sessLog.Info("peer disconnected")
		} else {
			sessLog.Error("recv failed", "err", err)
		}
	case err := <-sendErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			sessLog.Error("send failed", "err", err)
		}
	case <-sessionCtx.Done():
	}
}

// runStdinSend reads lines from stdin and sends each as a chat envelope
func (a *App) runStdinSend(ctx context.Context, link *mesh.Link) error {
	scanner := bufio.NewScanner(os.Stdin)
	s := link.Session()
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		text := scanner.Text()
		if text == "" {
			fmt.Print("> ")
			continue
		}
		env, err := mesh.NewEnvelope(link.LocalID(), s.RemoteID(), "chat", []byte(text))
		if err != nil {
			return fmt.Errorf("build envelope: %w", err)
		}
		enc, err := s.Encrypt(env)
		if err != nil {
			return fmt.Errorf("encrypt: %w", err)
		}
		sctx, scancel := context.WithTimeout(ctx, sendTimeout)
		err = link.SendEnvelope(sctx, enc)
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
