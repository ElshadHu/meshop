package mesh

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
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
func (n *Node) Send(env Envelope) error {
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

	if _, err := n.conn.Write(header[:]); err != nil {
		return fmt.Errorf("mesh: write frame length %w", err)
	}
	if _, err := n.conn.Write(body); err != nil {
		return fmt.Errorf("mesh: write frame body: %w", err)
	}
	return nil
}

// Recv reads the next length-prefixed JSON frame and decodes it into an envelope
func (n *Node) Recv() (Envelope, error) {
	var header [lengthPrefixBytes]byte
	if _, err := io.ReadFull(n.conn, header[:]); err != nil {
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
