#!/usr/bin/env bash
# Produce a tracker backup: a small per-run snapshot (Postgres dump + the list
# of content keys it references) plus a shared, append-only blob pool.
#
#   scripts/backup.sh                 # -> ./backups/tracker-backup-<ts>.tar.gz
#   scripts/backup.sh --upload        # also push to BACKUP_S3_* (R2/S3)
#
# Blobs are immutable and named by their own sha256, so a blob already in the
# pool can never need re-fetching. That is what makes a run cost O(new blobs)
# rather than O(all blobs), and stores each blob once instead of BACKUP_KEEP
# times. The snapshot names the keys it needs; the pool holds the bytes.
#
# Restore with scripts/restore.sh, which reads keys.txt and pulls exactly those
# blobs from the pool. Snapshots written before this change embed a blobs/
# directory instead; restore.sh still handles those.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

# Under cron the PATH is minimal; find uv in its usual install locations.
command -v uv >/dev/null 2>&1 || \
  PATH="$HOME/.local/bin:$HOME/.cargo/bin:/home/linuxbrew/.linuxbrew/bin:/usr/local/bin:$PATH"
command -v uv >/dev/null 2>&1 || { echo "uv not found in PATH" >&2; exit 1; }

# Never let two runs overlap. The cron fires hourly; if a run ever outlives its
# slot (a large initial pool seed, a slow link), a second one starting on top of
# it would race the pool and the retention prune. Skip rather than pile up.
LOCK=${BACKUP_LOCK:-/tmp/tracker-backup.lock}
exec 9>"$LOCK"
flock -n 9 || { echo "another backup is still running — skipping this slot"; exit 0; }

PG_CONTAINER=${PG_CONTAINER:-tracker-postgres}
OUT_DIR=${BACKUP_DIR:-./backups}
# Shared across all snapshots; never pruned by retention.
PACKS=${BACKUP_PACKS_DIR:-$OUT_DIR/packs}
TS=$(date +%Y%m%d-%H%M%S)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$OUT_DIR"

# Flags: --upload pushes the tarball to the BACKUP_S3_* store; --if-changed
# skips the whole backup when nothing has changed since the last one (for cron).
UPLOAD=0; IF_CHANGED=0
for arg in "$@"; do
  case "$arg" in
    --upload) UPLOAD=1 ;;
    --if-changed) IF_CHANGED=1 ;;
    *) echo "usage: $0 [--upload] [--if-changed]" >&2; exit 2 ;;
  esac
done

# Cheap change fingerprint: content-bearing tables only (documents, revisions,
# tasks) — deliberately not actors/leases, which churn without content changes.
# Two state tiers: what the last successful backup captured, and what the last
# successful UPLOAD captured — so a local-only run can't make cron skip an
# upload R2 never received. --if-changed compares against the tier it's about
# to satisfy.
STATE_FILE="$OUT_DIR/.last-backup-state"
UPLOAD_STATE_FILE="$OUT_DIR/.last-upload-state"
[ "$UPLOAD" = 1 ] && CHECK_FILE="$UPLOAD_STATE_FILE" || CHECK_FILE="$STATE_FILE"
STATE=$(docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDATABASE" -Atc \
  "select coalesce((select max(updated_at)::text from documents),'-')
     ||'|'||(select count(*) from documents)
     ||'|'||(select count(*) from document_revisions)
     ||'|'||coalesce((select max(updated_at)::text from tasks),'-')
     ||'|'||(select count(*) from tasks)" | tr -d '[:space:]')
if [ "$IF_CHANGED" = 1 ] && [ -f "$CHECK_FILE" ] && [ "$STATE" = "$(cat "$CHECK_FILE")" ]; then
  echo "no content changes since last backup — skipping"
  exit 0
fi

# 1) Postgres dump FIRST (so every content_key it references already has a blob,
#    since writes are blob-first). Custom format for flexible pg_restore.
echo "1/5  pg_dump ($PGDATABASE)"
docker exec "$PG_CONTAINER" pg_dump -U "$PGUSER" -d "$PGDATABASE" -Fc > "$WORK/db.dump"

# 2) Fold new blobs into the packs. Runs AFTER the dump: writes are blob-first,
#    so every content_key the dump references already exists in the store by the
#    time the dump was taken. Folding after can only add extra blobs, never miss
#    a referenced one. Sealed packs are never reopened, so this costs O(new).
echo "2/5  fold blobs into packs"
docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDATABASE" -tA -c \
  "select distinct content_key from (
     select content_key from documents where content_key is not null and content_key <> ''
     union all
     select content_key from document_revisions where content_key is not null and content_key <> ''
   ) k order by 1" > "$WORK/keys.txt"
uv run --quiet scripts/blobpack.py fold "$OUT_DIR" < "$WORK/keys.txt"

# 3) A snapshot the packs cannot serve is not a backup. The index is the record
#    of which pack holds what, so confirm every key this snapshot needs is in it.
echo "3/5  check packs cover this snapshot"
uv run --quiet - "$OUT_DIR" "$WORK/keys.txt" <<'PYEOF'
import sys, os, hashlib
work, keyfile = sys.argv[1], sys.argv[2]
idx_path = os.path.join(work, "packs", "INDEX")
raw = open(idx_path, "rb").read()
want = open(idx_path + ".sha256").read().split()[0]
if hashlib.sha256(raw).hexdigest() != want:
    sys.exit("INDEX checksum mismatch — refusing to write a snapshot against it")
held = {l.split(" ", 1)[0] for l in raw.decode().splitlines() if l.strip()}
need = {l.strip() for l in open(keyfile) if l.strip()}
absent = need - held
if absent:
    sys.exit(f"packs are missing {len(absent)} referenced blob(s) — aborting")
print(f"     {len(need)} keys, all present in packs")
PYEOF

echo "4/5  manifest"
DOCS=$(docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDATABASE" -tA -c "select count(*) from documents")
BLOBS=$(wc -l < "$WORK/keys.txt" | tr -d ' ')
PACK_N=$(ls -1 "$PACKS"/blobs-*.tar 2>/dev/null | wc -l | tr -d ' ')
HOST_ADDR=$(echo "$LISTEN_ADDR" | cut -d, -f1)
BINVER=$(curl -s --max-time 3 "http://$HOST_ADDR/version" | sed -n 's/.*"version":"\([^"]*\)".*/\1/p')
[ -n "$BINVER" ] || BINVER=$(git describe --tags --always --dirty 2>/dev/null || echo unknown)
cat > "$WORK/manifest.json" <<EOF
{
  "created_at": "$(date -Iseconds)",
  "binary_version": "$BINVER",
  "git_commit": "$(git rev-parse --short HEAD 2>/dev/null || echo unknown)",
  "documents": $DOCS,
  "blobs": $BLOBS,
  "packs": $PACK_N,
  "format": "pack-v1",
  "pg_dump_format": "custom",
  "storage_type": "${STORAGE_TYPE:-file}",
  "content_bucket": "${S3_BUCKET:-local}"
}
EOF

# 4) Bundle.
echo "5/5  tar"
TAR="$OUT_DIR/tracker-backup-$TS.tar.gz"
tar czf "$TAR" -C "$WORK" db.dump keys.txt manifest.json
echo "backup ready: $TAR ($(du -h "$TAR" | cut -f1)) — $DOCS docs, $BLOBS keys; $PACK_N pack(s) ($(du -sh "$PACKS" | cut -f1))"

# Restore instructions travel WITH the backup, local copy included: a directory
# of snapshots is no use to someone who does not know a snapshot holds no blob
# bytes. Regenerated every run so it cannot drift from the format it describes,
# and free of secrets — only variable names, never values.
{
  sed -n '1,/<!-- FACTS -->/p' scripts/restore-instructions.md | sed '$d'
  cat <<FACTS
## This instance, as of the latest backup

| | |
|---|---|
| written | $(date -Iseconds) |
| local snapshots | $OUT_DIR |
| local packs | $OUT_DIR/packs |
| bucket / prefix | \`${BACKUP_S3_BUCKET:-<none>}\` / \`${BACKUP_S3_PREFIX:-<none>}\` |
| newest snapshot | \`$(basename "$TAR")\` |
| documents | $DOCS |
| content keys | $BLOBS |
| packs | $PACK_N |
| snapshot format | pool-v2 (db.dump + keys.txt; blobs live in the pool) |
| tracker version | $BINVER |
| image | ghcr.io/chicagobuss/tracker:$BINVER |
| source | https://github.com/chicagobuss/tracker |
FACTS
  sed -n '/<!-- FACTS -->/,$p' scripts/restore-instructions.md | tail -n +2
} > "$OUT_DIR/RESTORE.md"

if [ "$UPLOAD" = 1 ]; then
  echo "uploading to backup store..."
  # The pool first: a snapshot offsite whose blobs are not offsite is not a
  # backup. Incremental, so this is O(new blobs) like the local sync.
  uv run --quiet scripts/blobpack.py push "$OUT_DIR"
  uv run --quiet scripts/s3util.py put-archive "$TAR"

  uv run --quiet scripts/s3util.py put-archive "$OUT_DIR/RESTORE.md" RESTORE.md
fi

# Record the fingerprint only after full success, so a failed run retries.
echo "$STATE" > "$STATE_FILE"
[ "$UPLOAD" = 1 ] && echo "$STATE" > "$UPLOAD_STATE_FILE"

# Local retention: keep the newest BACKUP_KEEP tarballs (default 48 ≈ 2 days
# of hourly-when-changed). The remote store keeps its own copies.
KEEP="${BACKUP_KEEP:-48}"
ls -1t "$OUT_DIR"/tracker-backup-*.tar.gz 2>/dev/null | tail -n +$((KEEP + 1)) | xargs -r rm -f
