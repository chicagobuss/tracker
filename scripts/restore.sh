#!/usr/bin/env bash
# Restore tracker from a backup tarball (made by backup.sh). The tarball is the
# portable unit — it can come from a local path or from R2/S3 (--from-s3).
#
#   scripts/restore.sh ./backups/tracker-backup-<ts>.tar.gz
#   scripts/restore.sh --from-s3 tracker-backup-<ts>.tar.gz
#   scripts/restore.sh <tarball> --db NAME --bucket NAME   # restore into scratch
#
# Restores Postgres + uploads blobs to the content store (or local directory). If
# restoring OVER the live database, stop the tracker container first.
#
# Two snapshot formats are accepted:
#   pack-v1  db.dump + keys.txt; blob bytes come from the pack files
#   pool-v2  db.dump + keys.txt; blob bytes come from the per-file pool
#   legacy   db.dump + an embedded blobs/ directory
# The format is detected from what is actually available, so older archives keep
# restoring for as long as the pool they reference is still on disk.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

PG_CONTAINER=${PG_CONTAINER:-tracker-postgres}
OUT_DIR=${BACKUP_DIR:-./backups}
PACKS=${BACKUP_PACKS_DIR:-$OUT_DIR/packs}
POOL=${BACKUP_POOL_DIR:-$OUT_DIR/pool}
SRC=""; FROM_S3=""; DB="$PGDATABASE"; BUCKET="$S3_BUCKET"
while [ $# -gt 0 ]; do
  case "$1" in
    --from-s3) FROM_S3="$2"; shift 2;;
    --db)      DB="$2"; shift 2;;
    --bucket)  BUCKET="$2"; shift 2;;
    *)         SRC="$1"; shift;;
  esac
done

WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT

if [ -n "$FROM_S3" ]; then
  SRC="$WORK/archive.tar.gz"
  uv run --quiet scripts/s3util.py get-archive "$FROM_S3" "$SRC"
fi
[ -f "${SRC:-}" ] || { echo "give a tarball path or --from-s3 NAME"; exit 1; }

echo "1/5  extract"
tar xzf "$SRC" -C "$WORK"
echo "     manifest: $(tr -d '\n ' < "$WORK/manifest.json")"

echo "2/5  ensure postgres + db ($DB)"
docker compose up -d postgres >/dev/null
for i in $(seq 1 30); do
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$PG_CONTAINER" 2>/dev/null)" = healthy ] && break; sleep 1
done
docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d postgres -tAc \
  "select 1 from pg_database where datname='$DB'" | grep -q 1 \
  || docker exec "$PG_CONTAINER" createdb -U "$PGUSER" "$DB"

echo "3/5  pg_restore -> $DB"
docker exec -i "$PG_CONTAINER" pg_restore -U "$PGUSER" -d "$DB" --clean --if-exists --no-owner < "$WORK/db.dump" 2>&1 \
  | grep -vE 'does not exist, skipping|errors ignored on restore' || true

echo "4/5  restoring blobs"
if [ -d "$WORK/blobs" ]; then
  # Legacy snapshot: the bytes travel inside the archive.
  echo "     legacy format (blobs embedded in archive)"
  if [ "${STORAGE_TYPE:-file}" = "file" ]; then
    mkdir -p "${BLOB_DIR:-./data/blobs}"
    cp -a "$WORK/blobs/." "${BLOB_DIR:-./data/blobs}/"
  else
    S3_BUCKET="$BUCKET" uv run --quiet scripts/s3util.py upload-blobs "$WORK/blobs"
  fi
elif [ -f "$WORK/keys.txt" ] && [ -f "$PACKS/INDEX" ]; then
  # pack-v1: blobpack refuses before writing anything if the packs cannot serve
  # every key, so a restore either completes or does not start.
  echo "     pack format — $(wc -l < "$WORK/keys.txt" | tr -d ' ') keys from $PACKS"
  S3_BUCKET="$BUCKET" uv run --quiet scripts/blobpack.py emit "$OUT_DIR" "$WORK/keys.txt"

elif [ -f "$WORK/keys.txt" ]; then
  # pool-v2: verify the pool can satisfy this snapshot BEFORE touching the
  # target store, so a restore either completes or does not start.
  echo "     pool format — $(wc -l < "$WORK/keys.txt" | tr -d ' ') keys from $POOL"
  [ -d "$POOL" ] || { echo "blob pool not found at $POOL" >&2; exit 1; }
  if [ "${STORAGE_TYPE:-file}" = "file" ]; then
    MISSING=0
    while IFS= read -r k; do [ -n "$k" ] && [ ! -f "$POOL/$k" ] && MISSING=$((MISSING+1)); done < "$WORK/keys.txt"
    [ "$MISSING" -eq 0 ] || { echo "pool is missing $MISSING of the keys this snapshot needs — refusing" >&2; exit 1; }
    mkdir -p "${BLOB_DIR:-./data/blobs}"
    while IFS= read -r k; do
      [ -n "$k" ] || continue
      mkdir -p "${BLOB_DIR:-./data/blobs}/$(dirname "$k")"
      cp -a "$POOL/$k" "${BLOB_DIR:-./data/blobs}/$k"
    done < "$WORK/keys.txt"
  else
    uv run --quiet scripts/s3util.py verify-pool "$POOL" "$WORK/keys.txt"
    S3_BUCKET="$BUCKET" uv run --quiet scripts/s3util.py upload-from-pool "$POOL" "$WORK/keys.txt"
  fi
else
  echo "     archive has neither blobs/ nor keys.txt — cannot restore content" >&2; exit 1
fi

echo "5/5  clear stale leases"
docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d "$DB" -tAc "delete from doc_locks;" >/dev/null 2>&1 || true

DOCS=$(docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d "$DB" -tAc "select count(*) from documents")
echo "restore complete: db=$DB ($DOCS docs), storage=${STORAGE_TYPE:-file}"
echo "  -> start the service: docker compose up -d tracker (with .env pointing at this db/storage)"
