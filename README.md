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


## Roadmap

|   | Goal                                                                  |
|---|-----------------------------------------------------------------------|
| + | Two parts of one Go program send a message to each other.             |
| + | Two computers on the same network send messages to each other.        |
| + | All messages are encrypted end-to-end.                                |
|   | Three or more devices, with messages relayed through intermediate peers. |
|   | Identity, contacts, and message history persist across restarts.      |
|   | Android library over Wi-Fi.                                           |
|   | Android over Bluetooth, no Wi-Fi or internet needed.                  |
|   | iPhone.                                                               |
|   | Web browser.                                                          |
|   | Developer documentation.                                              |
|   | Public API and external review.                                |
|   | 1.0 release.                                                          |

## Build and run

```
go build ./...
go vet ./...
go run ./cmd/demo
```

## Project layout

```
cmd/demo/         demo program
pkg/mesh/         the library
```
