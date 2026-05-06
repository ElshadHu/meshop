// chat message to each other over in-memory
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
	"syscall"
	"time"

	"github.com/ElshadHu/meshop/pkg/mesh"
)

const (
	dialTimeout      = 10 * time.Second
	keepAlivePeriod  = 30 * time.Second
	helloTimeout     = 5 * time.Second
	sendTimeout      = 5 * time.Second
	reconnectMinWait = 1 * time.Second
	reconnectMaxWait = 30 * time.Second
)

func main() {
	listenAddr := flag.String("listen", "", "address to listen on, for example :9000")
	dialAddr := flag.String("dial", "", "address to dial, e.g. 192.168.1.42:9000")
	flag.Parse()

	if (*listenAddr == "") == (*dialAddr == "") {
		log.Fatal("specify exactly one of --listen or --dial")
	}

	localID, err := mesh.NewPeerID()
	if err != nil {
		log.Fatalf("peer id: %v", err)
	}
	fmt.Printf("local peer id: %s\n", localID)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *listenAddr != "" {
		runListen(ctx, *listenAddr, localID)
	} else {
		runDial(ctx, *dialAddr, localID)
	}
}

// runListen accepts one connection at a time. After session ends it foes back to Accept

func runListen(ctx context.Context, addr string, localID mesh.PeerID) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}
	defer func() {
		_ = l.Close()
	}()
	fmt.Printf("listening on %s\n", l.Addr())

	// Close the listener in Ctrl+C
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
		fmt.Printf("\nPeer connected from %s\n", conn.RemoteAddr())
		configureTCP(conn)
		runSession(ctx, conn, localID)
		if ctx.Err() != nil {
			return
		}
		fmt.Println("waiting for next peer...")
	}
}

// runDial dials in a loop with exponential backoff

func runDial(ctx context.Context, addr string, localID mesh.PeerID) {
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
		runSession(ctx, conn, localID)
	}
}

// configureTCP enables TCP keepalive on the connection
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

// runSession runs full chat session over a connected conn
func runSession(ctx context.Context, conn net.Conn, localID mesh.PeerID) {
	node := mesh.NewNode(localID, conn)
	defer func() { _ = node.Close() }()
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	remoteID, err := exchangeHello(sessionCtx, node, localID)
	if err != nil {
		log.Printf("hello: %v", err)
		return
	}
	fmt.Printf("peer id: %s\n", remoteID)
	fmt.Print("> ")

	recvErr := make(chan error, 1)
	go func() {
		for {
			env, err := node.Recv(sessionCtx)
			if err != nil {
				recvErr <- err
				return
			}
			// \r erases the prompt, then I need to show it again
			fmt.Printf("\r<%s> %s\n> ", short(env.From), env.Payload)
		}
	}()

	sendErr := make(chan error, 1)
	go func() {
		sendErr <- runStdinSend(sessionCtx, node, localID, remoteID)
	}()
	select {
	case err := <-recvErr:
		if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
			fmt.Println("\n peer disconnected")
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

// exvhangeHello sends a hello envelope and reads one back, returning remote peer's ID
func exchangeHello(ctx context.Context, n *mesh.Node, localID mesh.PeerID) (mesh.PeerID, error) {
	hctx, cancel := context.WithTimeout(ctx, helloTimeout)
	defer cancel()

	out, err := mesh.NewEnvelope(localID, "", "hello", nil)
	if err != nil {
		return "", fmt.Errorf("build hello: %w", err)
	}
	if err := n.Send(hctx, out); err != nil {
		return "", fmt.Errorf("send hello: %w", err)
	}
	in, err := n.Recv(hctx)
	if err != nil {
		return "", fmt.Errorf("recv hello: %w", err)
	}
	if in.Type != "hello" {
		return "", fmt.Errorf("first envelope was %q, want %q", in.Type, "hello")
	}
	if in.From == "" {
		return "", fmt.Errorf("hello has empty From field")
	}
	return in.From, nil
}

// runStdinSend reads lines from stdin and sends each as a chat envelope
func runStdinSend(ctx context.Context, n *mesh.Node, localID, remoteID mesh.PeerID) error {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		text := scanner.Text()
		if text == "" {
			fmt.Printf("> ")
			continue
		}
		env, err := mesh.NewEnvelope(localID, remoteID, "chat", []byte(text))
		if err != nil {
			return fmt.Errorf("build envelope: %w", err)
		}
		sctx, scancel := context.WithTimeout(ctx, sendTimeout)
		err = n.Send(sctx, env)
		scancel()
		if err != nil {
			return fmt.Errorf("send: %w", err)
		}
		fmt.Printf("> ")
	}
	return scanner.Err()
}

// short returns the first 8 chars of a PeerID, for compact display.
func short(id mesh.PeerID) string {
	if len(id) <= 8 {
		return string(id)
	}
	return string(id[:8])
}
