# Version embedded into the binary: a git tag if HEAD is tagged, else the short
# sha, with -dirty when the tree has uncommitted changes.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: build image deploy version test clean

## build the local binary with the version stamped in
build:
	go build -trimpath -ldflags "-X main.version=$(VERSION)" -o tracker .

## build the docker image with the version stamped in
image:
	docker build --build-arg VERSION=$(VERSION) -t tracker:local .

## rebuild the image with the version + (re)start the container (no sudo)
deploy:
	TRACKER_VERSION=$(VERSION) docker compose up -d --build tracker

## print the version that would be embedded
version:
	@echo $(VERSION)

test:
	go test ./...

clean:
	rm -f tracker
