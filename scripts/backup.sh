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

PG_CONTAINER=${PG_CONTAINER:-coord-postgres}
OUT_DIR=${BACKUP_DIR:-./backups}
TS=$(date +%Y%m%d-%H%M%S)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$OUT_DIR"

# 1) Postgres dump FIRST (so every content_key it references already has a blob,
#    since writes are blob-first). Custom format for flexible pg_restore.
echo "1/4  pg_dump ($PGDATABASE)"
docker exec "$PG_CONTAINER" pg_dump -U "$PGUSER" -d "$PGDATABASE" -Fc > "$WORK/db.dump"

# 2) Then the blobs (immutable + content-addressed, so copying after the dump is
#    guaranteed to include everything the dump references).
echo "2/4  download blobs ($S3_BUCKET)"
mkdir -p "$WORK/blobs"
uv run --quiet scripts/s3util.py download-blobs "$WORK/blobs"

# 3) Manifest for sanity-checking a restore.
echo "3/4  manifest"
DOCS=$(docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDATABASE" -tA -c "select count(*) from documents")
BLOBS=$(find "$WORK/blobs" -type f | wc -l | tr -d ' ')
cat > "$WORK/manifest.json" <<EOF
{
  "created_at": "$(date -Iseconds)",
  "git_commit": "$(git rev-parse --short HEAD 2>/dev/null || echo unknown)",
  "documents": $DOCS,
  "blobs": $BLOBS,
  "pg_dump_format": "custom",
  "content_bucket": "$S3_BUCKET"
}
EOF

# 4) Bundle.
echo "4/4  tar"
TAR="$OUT_DIR/tracker-backup-$TS.tar.gz"
tar czf "$TAR" -C "$WORK" db.dump blobs manifest.json
echo "backup ready: $TAR ($(du -h "$TAR" | cut -f1)) — $DOCS docs, $BLOBS blobs"

if [ "${1:-}" = "--upload" ]; then
  echo "uploading to backup store..."
  uv run --quiet scripts/s3util.py put-archive "$TAR"
fi
