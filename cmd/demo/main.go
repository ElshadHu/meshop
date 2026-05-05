// chat message to each other over in-memory
package main

import (
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/ElshadHu/meshop/pkg/mesh"
)

func main() {
	yoyoID, err := mesh.NewPeerID()
	if err != nil {
		log.Fatalf("yoyo id: %v", err)
	}
	elijaID, err := mesh.NewPeerID()
	if err != nil {
		log.Fatalf("elija id: %v", err)
	}
	aConn, bConn := net.Pipe()
	yoyo := mesh.NewNode(yoyoID, aConn)
	elija := mesh.NewNode(elijaID, bConn)
	fmt.Printf("yoyo: %s\n", yoyoID)
	fmt.Printf("elija boi:   %s\n", elijaID)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); runYoyo(yoyo, elijaID) }()
	go func() { defer wg.Done(); runElija(elija, yoyoID) }()
	wg.Wait()

	if err := yoyo.Close(); err != nil {
		log.Printf("yoyo.Close: %v", err)
	}
	if err := elija.Close(); err != nil {
		log.Printf("elija boi.Close: %v", err)
	}
}

// runYoyo sends hello elija then waits for and prints Elija message
func runYoyo(n *mesh.Node, peer mesh.PeerID) {
	in, err := n.Recv()
	if err != nil {
		log.Fatalf("yoyo recv: %v", err)
	}
	fmt.Printf("yoyo receives from elija: %s\n", in.Payload)
	out, err := mesh.NewEnvelope(n.ID(), peer, "chat", []byte("hi elija"))
	if err != nil {
		log.Fatalf("yoyo envelope: %v", err)
	}
	if err := n.Send(out); err != nil {
		log.Fatalf("yoyo send: %v", err)
	}
	fmt.Printf("yoyo sends to elija boi: %s\n", out.Payload)
}

// runElija waits for yoyo's message, prints it , then sends a reply
func runElija(n *mesh.Node, peer mesh.PeerID) {
	out, err := mesh.NewEnvelope(n.ID(), peer, "chat", []byte("hello yoyo"))
	if err != nil {
		log.Fatalf("elija envelope: %v", err)
	}

	if err := n.Send(out); err != nil {
		log.Fatalf("elija sends: %v", err)
	}

	fmt.Printf("elija sent to yoyo: %s\n", out.Payload)
	in, err := n.Recv()
	if err != nil {
		log.Fatalf("elija recb: %v", err)
	}
	fmt.Printf("elija receives: %s\n", in.Payload)
}
