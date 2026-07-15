# Version embedded into the binary: a git tag if HEAD is tagged, else the short
# sha, with -dirty when the tree has uncommitted changes.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# Build from source rather than pulling the published image. If .env pins a
# custom stack via COMPOSE_FILE, pass no -f flags so compose honors that pin
# (explicit -f would silently override it). Still overridable: make deploy BUILD=...
COMPOSE_FILE_PINNED := $(shell sed -n 's/^COMPOSE_FILE=//p' .env 2>/dev/null)
BUILD ?= $(if $(COMPOSE_FILE_PINNED),,-f docker-compose.yml -f compose.build.yml)

.PHONY: help up down logs smoke build image deploy version test test-docker clean

## show this help
help:
	@awk '/^## /{doc=substr($$0,4); next} \
	      /^[a-z][a-z-]*:/{if(doc!=""){split($$0,t,":"); printf "  \033[36m%-12s\033[0m %s\n", t[1], doc; doc=""}}' \
	      $(MAKEFILE_LIST)

## start the stack (pulls the published image; no Go toolchain needed)
up:
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")
	docker compose up -d --wait
	@$(MAKE) --no-print-directory smoke

## add a welcome doc + example folio to a fresh instance (idempotent)
seed:
	@scripts/seed.sh

## stop the stack (data is preserved in the pgdata volume)
down:
	docker compose down

## follow the tracker logs
logs:
	docker compose logs -f tracker

## check that a running tracker is healthy and can round-trip a document
smoke:
	@scripts/smoke.sh

## build the local binary with the version stamped in
build:
	go build -trimpath -ldflags "-X main.version=$(VERSION)" -o tracker .

## build the docker image with the version stamped in
image:
	docker build --build-arg VERSION=$(VERSION) -t tracker:local .

## rebuild the image from source + (re)start the container (no sudo)
deploy:
	TRACKER_VERSION=$(VERSION) docker compose $(BUILD) up -d --build tracker

## print the version that would be embedded
version:
	@echo $(VERSION)

## run the tests against a throwaway postgres (needs a local Go toolchain)
test:
	@scripts/test.sh

## run the tests entirely in containers (no local Go toolchain needed)
test-docker:
	@scripts/test.sh --docker

## remove the built binary
clean:
	rm -f tracker
