// Package libmeshop is the gomobile bind surface for the meshop library
package libmeshop

import (
	"context"
	"path/filepath"

	"github.com/ElshadHu/meshop/pkg/mesh"
	_ "golang.org/x/mobile/bind"
)

// Logger is implemented on the Java side and called from Go
// gomobile generates a Java interface from this
type Logger interface {
	Log(line string)
}

// Message is the gomobile-friendly shape return by Recv
type Message struct {
	From    string
	Type    string
	Payload []byte
}

// Node is the only type Java sees besides Message and Logger
type Node struct {
	router *mesh.Router
}

// Start opens or creates the identity key at dataDir/key and returns a Node
func Start(dataDir string, log Logger) (*Node, error) {
	key, _, err := LoadOrCreateKey(filepath.Join(dataDir, "key"))
	if err != nil {
		return nil, err
	}
	return &Node{router: mesh.NewRouter(key, slogFromLogger(log))}, nil
}

// PeerID returns the local PeerID as a hex string
func (n *Node) PeerID() string {
	return string(n.router.LocalID())
}

// Listen binds TCP on addr and return the bound addr
func (n *Node) Listen(addr string) (string, error) {
	return n.router.Listen(addr)
}

// Connect dials addr and verifies the remote PeerID matches expected
func (n *Node) Connect(addr, ExpectedPeerID string) error {
	return n.router.Connect(context.Background(), addr, mesh.PeerID(ExpectedPeerID))
}

// Send encrypts and ships payload to peerID
func (n *Node) Send(peerID, msgType string, payload []byte) error {
	return n.router.Send(context.Background(), mesh.PeerID(peerID), msgType, payload)
}

// Recv blocks until a message arrives or the node is closed
func (n *Node) Recv() *Message {
	env, err := n.router.Recv(context.Background())
	if err != nil {
		return nil
	}
	return &Message{
		From:    string(env.From),
		Type:    env.Type,
		Payload: env.Payload,
	}
}

// Dropped return how many inbound messages were dropped
func (n *Node) Dropped() int64 {
	return int64(n.router.Dropped())
}

// Close shuts the node down
func (n *Node) Close() {
	_ = n.router.Close()
}
