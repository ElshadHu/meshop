package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ElshadHu/meshop/pkg/mesh"
)

const (
	dialTimeout    = 10 * time.Second
	sendTimeout    = 15 * time.Second
	keyFileMode    = 0o600
	keyFileDirMode = 0o700
)

type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ",") }
func (s *stringSlice) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func main() {
	listenAddr := flag.String("listen", "", "address to listen on, e.g. :9000")
	var connects stringSlice
	flag.Var(&connects, "connect", "neighbor to dial, formatted host:port=PEERID. Repeatable.")
	targetID := flag.String("target", "", "PeerID of the peer to chat with")
	keyPath := flag.String("key", defaultKeyPath(), "path to static identity key file")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	flag.CommandLine.SetOutput(os.Stderr)
	flag.Parse()
	if *listenAddr == "" && len(connects) == 0 {
		usage("at least one of --listen or --connect required")
	}
	level, err := parseLogLevel(*logLevel)
	if err != nil {
		usage(err.Error())
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level}))
	key, fresh, err := loadOrCreateKey(*keyPath)
	if err != nil {
		logger.Error("key load failed", "path", *keyPath, "err", err)
		os.Exit(1)
	}
	if fresh {
		logger.Info("identity created", "path", *keyPath)
	}
	logger.Info("local peer id", "id", key.PeerID())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	router := mesh.NewRouter(key, logger)
	defer router.Close()

	if *listenAddr != "" {
		bound, err := router.Listen(*listenAddr)
		if err != nil {
			logger.Error("listen failed", "addr", *listenAddr, "err", err)
			os.Exit(1)
		}
		logger.Info("listening", "addr", bound)
	}

	for _, spec := range connects {
		addr, peerID, ok := strings.Cut(spec, "=")
		if !ok {
			usage(fmt.Sprintf("--connect needs host:port=PEERID, got %q", spec))
		}
		dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
		err := router.Connect(dialCtx, addr, mesh.PeerID(peerID))
		cancel()
		if err != nil {
			logger.Error("connect failed", "addr", addr, "err", err)
			os.Exit(1)
		}
		logger.Info("connected", "addr", addr, "peer", peerID)
	}

	go recvLoop(ctx, router, logger)
	if *targetID != "" {
		fmt.Print("> ")
		sendLoop(ctx, router, mesh.PeerID(*targetID), logger)
	} else {
		<-ctx.Done()
	}
}

func recvLoop(ctx context.Context, router *mesh.Router, logger *slog.Logger) {
	for {
		env, err := router.Recv(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, mesh.ErrRouterClosed) {
				logger.Warn("recv ended", "err", err)
			}
			return
		}
		fmt.Printf("\r<%s> %s\n> ", short(env.From), env.Payload)
	}
}

func sendLoop(ctx context.Context, router *mesh.Router, target mesh.PeerID, logger *slog.Logger) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return
		}
		text := scanner.Text()
		if text == "" {
			fmt.Print("> ")
			continue
		}
		sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
		err := router.Send(sendCtx, target, "chat", []byte(text))
		cancel()
		if err != nil {
			logger.Warn("send failed", "err", err)
		}
		fmt.Print("> ")
	}
}

func usage(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	flag.Usage()
	os.Exit(2)
}

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

func short(id mesh.PeerID) string {
	if len(id) <= 8 {
		return string(id)
	}
	return string(id[:8])
}
