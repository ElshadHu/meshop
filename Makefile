.PHONY: build demo test vet fmt tidy clean

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
