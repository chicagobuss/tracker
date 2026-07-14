#!/usr/bin/env bash
# Run the test suite against a throwaway Postgres, so `make test` needs no setup
# and leaves nothing behind.
#
#   scripts/test.sh              # run tests with the local Go toolchain
#   scripts/test.sh --docker     # run them inside a Go container (no local Go needed)
#
# Extra args go to `go test` (e.g. scripts/test.sh -run TestAcquireLease -v).
set -euo pipefail

cd "$(dirname "$0")/.."

PG_IMAGE=pgvector/pgvector:pg17      # migrations need the pgvector extension
GO_IMAGE=golang:1.26-alpine
NET=tracker-test-net
PG=tracker-test-pg
PGPASS=test
PGDB=tracker_test

IN_DOCKER=0
if [[ "${1:-}" == "--docker" ]]; then
  IN_DOCKER=1
  shift
fi

cleanup() {
  docker rm -f "$PG" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup  # clear any leftovers from an interrupted run

docker network create "$NET" >/dev/null

echo "==> starting throwaway postgres ($PG_IMAGE)"
# Publish on an ephemeral host port so a real Postgres on 5432 isn't disturbed.
docker run -d --name "$PG" --network "$NET" \
  -e POSTGRES_PASSWORD="$PGPASS" -e POSTGRES_DB="$PGDB" \
  -p 127.0.0.1:0:5432 "$PG_IMAGE" >/dev/null

echo -n "==> waiting for postgres"
for _ in $(seq 1 60); do
  if docker exec "$PG" pg_isready -U postgres -d "$PGDB" >/dev/null 2>&1; then
    break
  fi
  echo -n "."
  sleep 1
done
echo
if ! docker exec "$PG" pg_isready -U postgres -d "$PGDB" >/dev/null 2>&1; then
  echo "postgres never became ready" >&2
  docker logs "$PG" >&2
  exit 1
fi

if [[ "$IN_DOCKER" == 1 ]]; then
  echo "==> go test (in $GO_IMAGE)"
  docker run --rm --network "$NET" \
    -v "$PWD:/src" -w /src \
    -v tracker-test-gomod:/go/pkg/mod \
    -e "TEST_DATABASE_URL=postgres://postgres:$PGPASS@$PG:5432/$PGDB?sslmode=disable" \
    "$GO_IMAGE" go test "$@" ./...
else
  if ! command -v go >/dev/null; then
    echo "go is not on PATH — install Go, or run: scripts/test.sh --docker" >&2
    exit 1
  fi
  PORT="$(docker port "$PG" 5432/tcp | head -1 | sed 's/.*://')"
  echo "==> go test (local toolchain, postgres on 127.0.0.1:$PORT)"
  TEST_DATABASE_URL="postgres://postgres:$PGPASS@127.0.0.1:$PORT/$PGDB?sslmode=disable" \
    go test "$@" ./...
fi
