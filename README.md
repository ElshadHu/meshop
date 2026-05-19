# meshop

Peer-to-peer encrypted mesh messaging library in Go. Two devices exchange
messages directly, without any server in between. When they are not in
direct range, messages hop through other nearby devices to reach the
recipient.


## What I want it to do

1. Send messages directly between two devices on the same network with
   no server.
2. Encrypt every message so only the sender and the receiver can read it.
3. Route messages through intermediate devices when the sender and
   receiver are not in range of each other.
4. Keep working when the user closes the app and opens it again: identity,
   contacts, and history are all preserved.
5. Run on phones over Bluetooth, with no internet at all. This
   is the original target.

## Status

This project is in early development. Like most software at this stage,
expect frequent and probably heavy changes: the public API, the wire
format, the on-disk layout, and the package layout itself will all move
as later goals land. Nothing is stable yet, and no version has been tagged.


## What works today

- Direct chat between two nodes on the same network, over TCP.
- End-to-end encryption with the Noise XX pattern (Curve25519, ChaCha20-Poly1305, SHA-256).
- Mesh relay: a node forwards encrypted messages for peers it does not talk to directly, with TTL and duplicate suppression.
- On-demand handshake over the relay when the sender and receiver have no direct link.
- Handshake-collision resolution when both peers start a handshake at the same time (bigger PeerID keeps the initiator role).
- Identity persists across restarts: the demo writes the static key to a file on first run and loads it on the next, so a node keeps the same PeerID.
- Inbox backpressure: when the receive buffer is full, the dropped count is exposed via `Router.Dropped()` and logged.
- CI runs `go vet`, `go build`, and `go test ./...` on every push.

## Build and run

```
make build      # compile everything
make test       # run unit tests
make vet        # static checks
make fmt        # format code
make demo       # build the demo binary
```

## Try the chat demo

The demo program is `cmd/demo`. Each node has:

- a **key file** — its identity. Same key file = same PeerID.
- a **PeerID** — long hex string printed on the first log line.
- optional `--listen :PORT` — accept neighbors.
- optional `--connect host:port=PEERID` — dial a neighbor.
- optional `--target PEERID` — who you want to chat with. Without this, you can only receive.

The `Makefile` wraps these with three helpers: `alice`, `bob`, `carol`. They use
key files in `/tmp/` so PeerIDs stay the same between runs.

### Two-node chat (Alice ↔ Bob)

Open two terminals.

1. **Terminal 1** — start Alice once to get her PeerID:
   ```
   make alice
   ```
   Copy the `id=...` value (call it `ALICE_ID`). Press Ctrl+C.

2. **Terminal 2** — start Bob to get his PeerID:
   ```
   make bob PEER=<ALICE_ID>
   ```
   Copy Bob's `id=...` (call it `BOB_ID`). Press Ctrl+C.

3. **Restart both with `TARGET`** so you can type:
   ```
   # Terminal 1
   make alice TARGET=<BOB_ID>

   # Terminal 2
   make bob PEER=<ALICE_ID> TARGET=<ALICE_ID>
   ```

Type a message in either terminal and press Enter. It shows up on the other side as `<peer_id_short> message`.

### Three-node chat with relay (Alice <-> Bob <-> Carol)

Carol has **no direct link** to Alice. Bob relays.

1. Get Carol's ID first:
   ```
   make carol PEER=<BOB_ID>
   ```
   Copy `CAROL_ID`. Press Ctrl+C.

2. Run all three:
   ```
   # Terminal 1
   make alice TARGET=<CAROL_ID>

   # Terminal 2
   make bob PEER=<ALICE_ID>

   # Terminal 3
   make carol PEER=<BOB_ID> TARGET=<ALICE_ID>
   ```

Type in Alice -> message reaches Carol through Bob. Type in Carol -> reaches Alice the same way. Bob only forwards.

To prove the relay matters: press Ctrl+C on Bob. Alice and Carol stop hearing each other. Restart Bob and it works again.

### Reset

```
make clean-keys   # delete /tmp/{alice,bob,carol}.key — new PeerIDs next run
```

## Project layout

```
cmd/demo/         demo program
pkg/mesh/         the library
```

## More details

If you want to see the architecture, the wire format, the design decisions, and the diagrams, visit the [wiki](https://github.com/ElshadHu/meshop/wiki).
