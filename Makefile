GO ?= go
export GOTOOLCHAIN := go1.27.4
export GOWORK := off
export GOMAXPROCS := 2

.PHONY: test test-race build vet
test:
	$(GO) test -mod=readonly -p 2 -count=1 ./...
test-race:
	$(GO) test -mod=readonly -race -p 2 -count=1 ./...
vet:
	$(GO) vet ./...
build:
	$(GO) build -mod=readonly -trimpath -o bin/bloem-storage-s3 ./cmd/bloem-storage-s3
