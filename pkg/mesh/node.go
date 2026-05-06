package mesh

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// MaxFrameBytes caps a single wire frame at 1 MIB. Receivers refuse larger
// frames before allocating
const MaxFrameBytes = 1 << 20

var ErrFrameTooLarge = errors.New("mesh: frame exceeds MaxFrameBytes")

// lengthPrefixBytes is the size of the wire-framing length prefix
const lengthPrefixBytes = 4

// Node is one end of a point-to-point mesh connection
type Node struct {
	id     PeerID
	conn   net.Conn
	sendMu sync.Mutex
}

// NewNode returns a Node that uses conn for transport
func NewNode(id PeerID, conn net.Conn) *Node {
	return &Node{id: id, conn: conn}
}

// ID returns this Node's PeerID
func (n *Node) ID() PeerID {
	return n.id
}

// Send writes  to the underlying conn as a length-prefixed JSON frame
// it will serialises concurrent calls with an internal mutex
func (n *Node) Send(ctx context.Context, env Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("mesh: marshal envelope: %w", err)
	}
	if len(body) > MaxFrameBytes {
		return fmt.Errorf("mesh: send: %w (size %d)", ErrFrameTooLarge, len(body))
	}

	var header [lengthPrefixBytes]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(body)))
	n.sendMu.Lock()
	defer n.sendMu.Unlock()

	// A previous cancelled call maybe left the deadline in the past, clear it
	_ = n.conn.SetWriteDeadline(time.Time{})

	stop := n.bindWriteCancel(ctx)
	defer stop()

	if _, err := n.conn.Write(header[:]); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("mesh: send: %w", ctxErr)
		}
		return fmt.Errorf("mesh: write frame length: %w", err)
	}
	if _, err := n.conn.Write(body); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("mesh: send: %w", ctxErr)
		}
		return fmt.Errorf("mesh: write frame body: %w", err)
	}
	return nil
}

// Recv reads the next length-prefixed JSON frame and decodes it into an envelope
func (n *Node) Recv(ctx context.Context) (Envelope, error) {
	_ = n.conn.SetReadDeadline(time.Time{})
	stop := n.bindReadCancel(ctx)
	defer stop()
	var header [lengthPrefixBytes]byte
	if _, err := io.ReadFull(n.conn, header[:]); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Envelope{}, fmt.Errorf("mesh: recv: %w", ctxErr)
		}
		if errors.Is(err, io.EOF) {
			return Envelope{}, io.EOF
		}
		return Envelope{}, fmt.Errorf("mesh: read frame length: %w", err)
	}

	length := binary.BigEndian.Uint32(header[:])
	if length > MaxFrameBytes {
		return Envelope{}, fmt.Errorf("mesh: recv: %w (size %d)", ErrFrameTooLarge, length)
	}
	body := make([]byte, length)

	if _, err := io.ReadFull(n.conn, body); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Envelope{}, fmt.Errorf("mesh: recv: %w", ctxErr)
		}
		return Envelope{}, fmt.Errorf("mesh: read frame body: %w", err)
	}

	var env Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return Envelope{}, fmt.Errorf("mesh: decode envelope: %w", err)
	}
	return env, nil
}

// Close closes the underlying connection.
func (n *Node) Close() error {
	if err := n.conn.Close(); err != nil {
		return fmt.Errorf("mesh: close: %w", err)
	}
	return nil
}

// bindReadCancel spawns a watch goroutine that sets the conn's read deadline to time.Now()
func (n *Node) bindReadCancel(ctx context.Context) func() {
	if ctx == nil {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = n.conn.SetReadDeadline(time.Now())
		case <-done:
		}
	}()
	return func() { close(done) }
}

// bindWriteCancel is the write-side of bindReadCancel. Writes use a separate watch so a cancelled send doesn't push
func (n *Node) bindWriteCancel(ctx context.Context) func() {
	if ctx == nil {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = n.conn.SetWriteDeadline(time.Now())
		case <-done:
		}
	}()
	return func() { close(done) }
}
