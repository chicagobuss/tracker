#!/usr/bin/env bash
# Produce a self-contained tracker backup: Postgres dump + content blobs +
# manifest, as one tar.gz. Optionally upload it to R2/S3 (BACKUP_S3_* env).
#
#   scripts/backup.sh                 # -> ./backups/tracker-backup-<ts>.tar.gz
#   scripts/backup.sh --upload        # also push to BACKUP_S3_* (R2/S3)
#
# Restore with scripts/restore.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

# Under cron the PATH is minimal; find uv in its usual install locations.
command -v uv >/dev/null 2>&1 || \
  PATH="$HOME/.local/bin:$HOME/.cargo/bin:/home/linuxbrew/.linuxbrew/bin:/usr/local/bin:$PATH"
command -v uv >/dev/null 2>&1 || { echo "uv not found in PATH" >&2; exit 1; }

PG_CONTAINER=${PG_CONTAINER:-tracker-postgres}
OUT_DIR=${BACKUP_DIR:-./backups}
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
echo "1/4  pg_dump ($PGDATABASE)"
docker exec "$PG_CONTAINER" pg_dump -U "$PGUSER" -d "$PGDATABASE" -Fc > "$WORK/db.dump"

# 2) Then the blobs (immutable + content-addressed, so copying after the dump is
#    guaranteed to include everything the dump references).
echo "2/4  copying blobs"
mkdir -p "$WORK/blobs"
if [ "${STORAGE_TYPE:-file}" = "file" ]; then
  if [ -d "${BLOB_DIR:-./data/blobs}" ]; then
    cp -a "${BLOB_DIR:-./data/blobs}/." "$WORK/blobs/"
  fi
else
  uv run --quiet scripts/s3util.py download-blobs "$WORK/blobs"
fi

# 3) Manifest for sanity-checking a restore. binary_version is the version the
#    LIVE service reports (the binary that produced this data); fall back to the
#    repo's git version if the service isn't reachable.
echo "3/4  manifest"
DOCS=$(docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDATABASE" -tA -c "select count(*) from documents")
BLOBS=$(find "$WORK/blobs" -type f | wc -l | tr -d ' ')
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
  "pg_dump_format": "custom",
  "storage_type": "${STORAGE_TYPE:-file}",
  "content_bucket": "${S3_BUCKET:-local}"
}
EOF

# 4) Bundle.
echo "4/4  tar"
TAR="$OUT_DIR/tracker-backup-$TS.tar.gz"
tar czf "$TAR" -C "$WORK" db.dump blobs manifest.json
echo "backup ready: $TAR ($(du -h "$TAR" | cut -f1)) — $DOCS docs, $BLOBS blobs"

if [ "$UPLOAD" = 1 ]; then
  echo "uploading to backup store..."
  uv run --quiet scripts/s3util.py put-archive "$TAR"
fi

# Record the fingerprint only after full success, so a failed run retries.
echo "$STATE" > "$STATE_FILE"
[ "$UPLOAD" = 1 ] && echo "$STATE" > "$UPLOAD_STATE_FILE"

# Local retention: keep the newest BACKUP_KEEP tarballs (default 48 ≈ 2 days
# of hourly-when-changed). The remote store keeps its own copies.
KEEP="${BACKUP_KEEP:-48}"
ls -1t "$OUT_DIR"/tracker-backup-*.tar.gz 2>/dev/null | tail -n +$((KEEP + 1)) | xargs -r rm -f
