#!/usr/bin/env bash
# Seed a fresh tracker with a welcome document and a small example folio, so a new
# instance isn't an empty void — an agent that lands on it can read `welcome` and
# bootstrap itself, and a human can see what documents and folios actually are.
#
#   scripts/seed.sh                      # uses BASE_URL / API_TOKENS from .env
#   BASE_URL=http://host:8770 scripts/seed.sh
#
# Idempotent: it skips anything that already exists, and it never modifies or
# deletes a document you already have.
set -euo pipefail

cd "$(dirname "$0")/.."

# Explicit env beats .env beats the default (sourcing .env would clobber the caller).
_env_base_url="${BASE_URL:-}"
_env_api_tokens="${API_TOKENS:-}"
[[ -f .env ]] && set -a && . ./.env && set +a
BASE_URL="${_env_base_url:-${BASE_URL:-http://127.0.0.1:8770}}"
API_TOKENS="${_env_api_tokens:-${API_TOKENS:-}}"

ACTOR="${ACTOR:-seed}"
TOKEN="${API_TOKENS%%,*}"
AUTH=()
[[ -n "$TOKEN" ]] && AUTH=(-H "Authorization: Bearer $TOKEN")

# post PATH JSON -> prints "created" / "exists", fatal on anything else.
post() {
  local path="$1" body="$2" out code
  out="$(curl -sS -X POST "$BASE_URL$path" \
    -H "X-Actor: $ACTOR" -H 'Content-Type: application/json' "${AUTH[@]}" \
    -d "$body" -w $'\n%{http_code}')"
  code="${out##*$'\n'}"
  case "$code" in
    2*) echo "created" ;;
    # A duplicate slug means a previous seed (or the user) already made it.
    409) echo "exists" ;;
    *)
      echo "FAIL: POST $path -> HTTP $code" >&2
      echo "${out%$'\n'*}" >&2
      exit 1 ;;
  esac
}

# json_doc SLUG TITLE KIND TAGS_JSON CONTENT_FILE -> a create_doc body
json_doc() {
  python3 -c '
import json, sys
slug, title, kind, tags, path = sys.argv[1:6]
print(json.dumps({
    "slug": slug, "title": title, "kind": kind,
    "tags": json.loads(tags), "content_type": "text/markdown",
    "content": open(path).read(),
}))' "$@"
}

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# --- welcome: what an agent reads first -------------------------------------
cat > "$WORK/welcome.md" <<'MD'
# Welcome to tracker

You are (probably) a coding agent. This document is here so you can bootstrap
yourself without asking anyone. tracker is the **shared source of truth** for a
fleet of agents: what we decided, what we're doing, and who is editing what right
now. If several agents work on the same codebase, this is how they avoid writing
over each other.

## The four ideas

- **document** — one markdown file with a slug, content, versions, and
  attribution. Everything is a document.
- **folio** — a small collection of related documents (what a gist used to be). A
  folio is itself a document; its files are documents tagged `folio:<slug>` with
  slugs like `<folio>/<file>.md`.
- **lease** — a TTL'd "I am writing this right now" lock on a document. It answers
  the question a shared store must answer: *is someone else already editing this?*
  A crashed agent's lease expires on its own, so nothing is blocked forever.
- **actor** — who you are. Every write is stamped with your `X-Actor` name. It is
  attribution, not authentication: be honest.

## How to use it

Read before you work. Write what others need.

1. **Orient.** `list_docs(q="...")` to search (quoted `"phrases"`, `OR`,
   `-negation`; bare words must all match), `list_folios()` to browse, `list_tags()`
   to learn the vocabulary. Then `get_doc(id, include_content=true)` to read.
2. **Check before editing a shared doc.** `lock_status(id)` tells you whether
   another agent holds the lease. If it does, do something else — don't fight it.
3. **Write.** `update_doc(id, content)` does the whole dance for you: it takes the
   lease, writes with a version check, and releases. If someone else holds the
   lease, or the document moved under you, it fails cleanly *without writing*.
   That's coordination working, not an error to route around.
4. **Record outcomes** other agents will need: decisions, gotchas, and the shape
   of what you changed. A note nobody can find is a note that doesn't exist —
   tag it (`topic:*`, `status:*`) and give it a slug someone would guess.

## Signals, not bugs

- `lease_held` — another agent is writing it. Back off.
- `version_conflict` — it changed while you were thinking. Re-read, merge on
  purpose, retry.
- `deleted` — it's soft-deleted. Restore it before writing.

## Deleting

Prefer `soft_delete_doc` — it hides the document from search but keeps the row and
its history, and `restore_doc` brings it back. `hard_delete_doc` is irreversible
and requires `confirm` to exactly equal the slug. Think before you use it.

## Where to look next

- `getting-started/` — the folio next door: the coordination model in detail, and
  how to point an agent at this instance.
- `GET /openapi.yaml` — the full, authoritative API.
- `GET /llms.txt` — this server's own machine-readable index.

Delete this document once your instance has real content in it. It's a seed, not
furniture.
MD

# --- the getting-started folio ----------------------------------------------
cat > "$WORK/coordination-model.md" <<'MD'
# The coordination model

tracker exists to answer one question that a git repo and a wiki both answer
badly: **is another agent writing this right now?**

## Two layers, both required

A write must satisfy both, or it is rejected:

1. **A lease you hold.** `POST /docs/{id}/lock` returns a `lease_token`. The lease
   has a TTL and belongs to one actor. A second agent asking for the same lease
   gets `409 lease_held` and is told who holds it.
2. **The version you read.** The write carries `If-Match: <version>`. If the
   document moved since you read it, you get `412 version_conflict` and nothing is
   written.

The lease stops two agents from working on the same doc at once. The version check
stops a *stale* write from landing even when the lease looks fine. You need both:
a lease alone can't tell you the content is still what you read.

Via MCP, `update_doc(id, content)` performs all of this server-side — take lease,
check version, write, release — so you rarely do it by hand.

## Why leases and not locks

A lock held by a crashed process is a lock held forever. A lease expires: the row
carries a TTL and a heartbeat, so a dead agent's grip on a document decays on its
own and the next writer simply takes it. That's the whole reason a coordination
store is a database problem and not a file-format problem.

## Tasks

The same idea, applied to work instead of documents. `enqueue_task` adds to the
queue; `claim_task` atomically takes the next one (Postgres `FOR UPDATE SKIP
LOCKED`, so two agents never get the same task); `complete_task` may only be called
by the actor that holds the claim. Claims expire too — a crashed worker's task
becomes claimable again, and `attempts` counts how often that has happened.
MD

cat > "$WORK/agent-setup.md" <<'MD'
# Pointing an agent at this instance

tracker serves MCP natively over Streamable HTTP at `POST /mcp`. There is no local
script to install and nothing to keep in sync: the tools ship inside the tracker
binary, so a client cannot drift from the server.

Replace `<base>` with this instance's URL (`http://127.0.0.1:8770` by default).

## Claude Code

```bash
claude mcp add --transport http --scope user tracker <base>/mcp \
  --header "X-Actor: claude-code-<host>"
```

## Cursor (`~/.cursor/mcp.json`)

```json
{ "mcpServers": { "tracker": {
    "url": "<base>/mcp",
    "headers": { "X-Actor": "cursor-<host>" } } } }
```

## Gemini CLI (`~/.gemini/settings.json`)

```json
{ "mcpServers": { "tracker": {
    "httpUrl": "<base>/mcp",
    "headers": { "X-Actor": "gemini-cli-<host>" } } } }
```

## Anything else

Any MCP client that speaks Streamable HTTP works: point its HTTP config (`url`,
`httpUrl`, or `serverUrl`, depending on the tool) at `<base>/mcp` and add the
`X-Actor` header. No command, args, or env blocks.

## X-Actor

Name yourself `<tool>-<host>` (e.g. `claude-code-laptop`). Every write records it:
document author, lease owner, task claimant. It is **self-asserted attribution**,
not authentication — on a trusted network that is the point. If the server has
`API_TOKENS` set, also send `Authorization: Bearer <token>`; that gates *access*,
while `X-Actor` records *identity*.

## Gotchas

- MCP tools load at session start: after registering, they appear in your **next**
  session. A running session can fall back to curl against the REST API.
- Folio files have multi-segment slugs (`<folio>/<file>.md`) and work by slug
  everywhere. The one exception: a file literally named `raw`, `lock`,
  `soft-delete`, or `restore` collides with those routes — address it via
  `/folios/{slug}/files/{filename}` or its UUID.
- A soft-deleted slug stays reserved until it is restored or hard-deleted.
MD

echo "==> seeding $BASE_URL"

printf '    welcome                            ... '
post /docs "$(json_doc welcome "Welcome to tracker — start here" note '["topic:tracker","seed"]' "$WORK/welcome.md")"

printf '    folio: getting-started             ... '
post /folios '{"slug":"getting-started","title":"Getting started with tracker","description":"How coordination works here, and how to point an agent at this instance."}'

seed_file() { # seed_file BASENAME TITLE
  printf '    getting-started/%-22s ... ' "$1.md"
  post /folios/getting-started/files "$(python3 -c '
import json, sys
print(json.dumps({"filename": sys.argv[1], "title": sys.argv[2],
                  "content_type": "text/markdown", "content": open(sys.argv[3]).read()}))' \
    "$1.md" "$2" "$WORK/$1.md")"
}
seed_file coordination-model "The coordination model — leases, versions, and tasks"
seed_file agent-setup        "Pointing an agent at this instance"

echo "==> seeded. Open $BASE_URL (or: curl $BASE_URL/docs/welcome/raw)"
