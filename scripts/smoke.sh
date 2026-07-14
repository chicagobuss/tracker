#!/usr/bin/env bash
# Prove a running tracker actually works: health, then a full write round-trip
# through the lease + version-check path (create -> lock -> write -> read back),
# then clean up after itself.
#
#   scripts/smoke.sh                     # uses BASE_URL / API_TOKENS from .env
#   BASE_URL=http://host:8770 scripts/smoke.sh
set -euo pipefail

cd "$(dirname "$0")/.."

# Sourcing .env would clobber anything the caller set on the command line, so
# remember those first and put them back afterwards: explicit env beats .env
# beats the default.
_env_base_url="${BASE_URL:-}"
_env_api_tokens="${API_TOKENS:-}"
[[ -f .env ]] && set -a && . ./.env && set +a
BASE_URL="${_env_base_url:-${BASE_URL:-http://127.0.0.1:8770}}"
API_TOKENS="${_env_api_tokens:-${API_TOKENS:-}}"

ACTOR="${ACTOR:-smoke-test}"
SLUG="smoke-test-$$"
# Tracker has to reach Postgres and run migrations before it listens, so a smoke
# run straight after `docker compose up -d` needs to wait rather than fail.
READY_TIMEOUT="${READY_TIMEOUT:-60}"

# API_TOKENS may hold several comma-separated tokens; any one of them works.
TOKEN="${API_TOKENS%%,*}"
AUTH=()
[[ -n "$TOKEN" ]] && AUTH=(-H "Authorization: Bearer $TOKEN")

api() { # api METHOD PATH [curl args...] -> body on stdout, non-2xx is fatal
  local method="$1" path="$2"; shift 2
  local out code
  out="$(curl -sS -X "$method" "$BASE_URL$path" \
    -H "X-Actor: $ACTOR" "${AUTH[@]}" "$@" -w $'\n%{http_code}')"
  code="${out##*$'\n'}"
  out="${out%$'\n'*}"
  if [[ "$code" != 2* ]]; then
    echo "FAIL: $method $path -> HTTP $code" >&2
    echo "$out" >&2
    exit 1
  fi
  printf '%s' "$out"
}

# Pull a dotted-path field out of a JSON object, without requiring jq.
field() { # field a.b.c '{"a":{"b":{"c":1}}}'
  python3 -c '
import json, sys
data = json.loads(sys.argv[2])
for part in sys.argv[1].split("."):
    data = data[part]
print(data)
' "$1" "$2"
}

echo "==> $BASE_URL"

# Wait for tracker to be ready, and establish that the thing answering at BASE_URL
# really is tracker. Two failure modes to keep apart:
#   - nothing listening yet — the normal case right after `docker compose up -d`,
#     since tracker still has to reach Postgres and run migrations. Worth waiting.
#   - something listening that isn't tracker — a squatted port. Waiting won't help,
#     and reporting *its* errors as tracker's sends people after the wrong process.
# Progress goes to stderr; stdout is the health body the caller captures.
preflight() {
  local deadline=$((SECONDS + READY_TIMEOUT)) out code body waiting=0 last=""

  while :; do
    if out="$(curl -sS -m 5 "$BASE_URL/healthz" -w $'\n%{http_code}' 2>/dev/null)"; then
      code="${out##*$'\n'}"
      body="${out%$'\n'*}"

      # A JSON body with a status field, 2xx: tracker, up.
      if [[ "$code" == 2* ]] && field status "$body" >/dev/null 2>&1; then
        (( waiting )) && echo >&2
        printf '%s' "$body"
        return
      fi

      # JSON, but not healthy yet (e.g. 503 while Postgres is still starting):
      # that's tracker booting, so keep waiting.
      if python3 -c 'import json,sys; json.loads(sys.argv[1])' "$body" 2>/dev/null; then
        last="HTTP $code: $body"
      else
        # Not JSON at all — some other service owns this port. Fail now.
        (( waiting )) && echo >&2
        cat >&2 <<EOF
FAIL: something is listening at $BASE_URL, but it is not tracker (HTTP $code).

  Another service already owns that port. Move tracker to a free one in .env:
      PORT=8771
      BASE_URL=http://127.0.0.1:8771
  then re-run: docker compose up -d && scripts/smoke.sh

  It said:
$(printf '%s' "$body" | head -3 | sed 's/^/      /')
EOF
        exit 1
      fi
    fi

    if (( SECONDS >= deadline )); then
      (( waiting )) && echo >&2
      cat >&2 <<EOF
FAIL: tracker did not become ready at $BASE_URL within ${READY_TIMEOUT}s.
${last:+
  Last response: $last}
  Check the stack:
      docker compose ps
      docker compose logs tracker

  If the container failed to bind because the port was taken, pick another in .env:
      PORT=8771
      BASE_URL=http://127.0.0.1:8771
EOF
      exit 1
    fi

    if (( ! waiting )); then
      printf '    waiting for tracker to come up' >&2
      waiting=1
    fi
    printf '.' >&2
    sleep 1
  done
}

health="$(preflight)"
echo "    health:  $(field status "$health") (tracker $(field version "$health"))"
if [[ -z "${TOKEN:-}" ]]; then
  echo "    auth:    DISABLED (API_TOKENS is empty — fine for loopback, not for a network)"
fi

# 1. create
doc="$(api POST /docs -H 'Content-Type: application/json' \
  -d "{\"slug\":\"$SLUG\",\"title\":\"Smoke test\",\"kind\":\"note\",\"tags\":[\"smoke\"]}")"
id="$(field document.id "$doc")"
version="$(field document.version "$doc")"
echo "    create:  $SLUG (v$version)"

# 2. lock — a write needs a live lease held by this actor
lock="$(api POST "/docs/$id/lock" -H 'Content-Type: application/json' \
  -d '{"reason":"smoke test","ttl_seconds":60}')"
lease="$(field lock.lease_token "$lock")"
echo "    lock:    acquired"

# 3. write — lease token + If-Match base version
body='# smoke test

If you can read this, the write path works.'
written="$(api PUT "/docs/$id" \
  -H "X-Lease-Token: $lease" -H "If-Match: $version" \
  -H 'Content-Type: text/markdown' --data-binary "$body")"
newversion="$(field document.version "$written")"
echo "    write:   v$version -> v$newversion"

# 4. read back and compare
got="$(api GET "/docs/$id/raw")"
if [[ "$got" != "$body" ]]; then
  echo "FAIL: content round-trip mismatch" >&2
  diff <(printf '%s' "$body") <(printf '%s' "$got") >&2 || true
  exit 1
fi
echo "    read:    content matches"

# 5. clean up (hard delete requires the slug as confirmation)
api DELETE "/docs/$id" -H 'Content-Type: application/json' \
  -d "{\"confirm\":\"$SLUG\"}" >/dev/null
echo "    cleanup: removed $SLUG"

echo "==> smoke test passed"
