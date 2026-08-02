# Version embedded into the binary: a git tag if HEAD is tagged, else the short
# sha, with -dirty when the tree has uncommitted changes.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# Build from source rather than pulling the published image. If .env pins a
# custom stack via COMPOSE_FILE, pass no -f flags so compose honors that pin
# (explicit -f would silently override it). Still overridable: make deploy BUILD=...
COMPOSE_FILE_PINNED := $(shell sed -n 's/^COMPOSE_FILE=//p' .env 2>/dev/null)
BUILD ?= $(if $(COMPOSE_FILE_PINNED),,-f docker-compose.yml -f compose.build.yml)

# Where CI publishes. `make update TAG=vX.Y.Z` is how a host runs a release.
IMAGE ?= ghcr.io/chicagobuss/tracker

.PHONY: help up update status down logs smoke build image deploy version test test-docker clean

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

## run a published release on this host: make update TAG=v1.4.2
update:
	@test -n "$(TAG)" || { echo "usage: make update TAG=vX.Y.Z   (releases: https://github.com/chicagobuss/tracker/releases)"; exit 1; }
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")
	@case ":$(COMPOSE_FILE_PINNED):" in *compose.build.yml*) \
	  echo "refusing: COMPOSE_FILE in .env includes compose.build.yml, which pins image: tracker:local"; \
	  echo "and would override TRACKER_IMAGE with a local build. Drop it from COMPOSE_FILE to run releases,"; \
	  echo "or use 'make deploy' if this host is meant to build from source."; exit 1;; esac
	@if grep -q '^TRACKER_IMAGE=' .env; then \
	  tmp=$$(mktemp) && sed 's|^TRACKER_IMAGE=.*|TRACKER_IMAGE=$(IMAGE):$(TAG)|' .env > "$$tmp" \
	    && cat "$$tmp" > .env && rm -f "$$tmp"; \
	else printf 'TRACKER_IMAGE=%s:%s\n' '$(IMAGE)' '$(TAG)' >> .env; fi
	@echo "pinned TRACKER_IMAGE=$(IMAGE):$(TAG) in .env"
	docker compose pull tracker
	@$(MAKE) --no-print-directory up
	@$(MAKE) --no-print-directory status

## show what this host is pinned to vs what it is actually running
status:
	@printf 'pinned:  %s\n' "$$(sed -n 's/^TRACKER_IMAGE=//p' .env 2>/dev/null || true)"
	@printf 'running: '; \
	  url="$$(sed -n 's/^BASE_URL=//p' .env 2>/dev/null | head -1)"; \
	  curl -fsS --connect-timeout 3 --max-time 5 "$${url:-http://127.0.0.1:8770}/version" 2>/dev/null || echo unreachable
	@echo

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
	@if grep -q '^TRACKER_IMAGE=' .env 2>/dev/null && [ -z "$(BUILD)" ]; then \
	  echo "refusing: this host is pinned to $$(sed -n 's/^TRACKER_IMAGE=//p' .env) and runs a published"; \
	  echo "image, so there is nothing to build — 'make deploy' would report success and change nothing."; \
	  echo "Use 'make update TAG=vX.Y.Z' to move it, or 'make deploy BUILD=-f docker-compose.yml -f compose.build.yml'"; \
	  echo "to deliberately build from source here."; exit 1; fi
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
