.PHONY: build demo test vet fmt tidy clean alice bob carol clean-keys

build:
	go build ./...

demo:
	go build -o demo ./cmd/demo

test:
	go test ./...

vet:
	go vet ./...

fmt:
	go fmt ./...

tidy:
	go mod tidy

clean:
	rm -f demo

# --- chat demo ---
# Usage:
#   make alice                          # just listen, print PeerID
#   make alice TARGET=<bob-id>          # listen + chat with bob
#   make bob   PEER=<alice-id>          # connect to alice
#   make bob   PEER=<alice-id> TARGET=<alice-id>
#   make carol PEER=<bob-id>   TARGET=<alice-id>

alice:
	go run ./cmd/demo --listen :9000 --key /tmp/alice.key $(if $(TARGET),--target $(TARGET))

bob:
	@test -n "$(PEER)" || (echo "PEER=<alice-peer-id> required" && exit 1)
	go run ./cmd/demo --listen :9001 --key /tmp/bob.key \
		--connect 127.0.0.1:9000=$(PEER) \
		$(if $(TARGET),--target $(TARGET))

carol:
	@test -n "$(PEER)" || (echo "PEER=<bob-peer-id> required" && exit 1)
	go run ./cmd/demo --key /tmp/carol.key \
		--connect 127.0.0.1:9001=$(PEER) \
		$(if $(TARGET),--target $(TARGET))

clean-keys:
	rm -f /tmp/alice.key /tmp/bob.key /tmp/carol.key
