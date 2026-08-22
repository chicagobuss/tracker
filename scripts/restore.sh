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
# Three snapshot formats are accepted:
#   pack-v1  db.dump + keys.txt; blob bytes come from the pack files
#   pool-v2  db.dump + keys.txt; blob bytes come from the per-file pool
#   legacy   db.dump + an embedded blobs/ directory
# The format is read from the snapshot's own manifest.json, not guessed from what
# happens to exist on this host, so a pool-era archive still restores from the
# pool even on a machine that has since started writing packs.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

PG_CONTAINER=${PG_CONTAINER:-tracker-postgres}
OUT_DIR=${BACKUP_DIR:-./backups}
PACKS=${BACKUP_PACKS_DIR:-$OUT_DIR/packs}
POOL=${BACKUP_POOL_DIR:-$OUT_DIR/pool}
# S3_BUCKET is commented out in the default (STORAGE_TYPE=file) .env, and this
# runs under `set -u`, so referencing it unguarded aborted every file-backend
# restore before argument parsing even began.
SRC=""; FROM_S3=""; DB="$PGDATABASE"; BUCKET="${S3_BUCKET:-}"
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
# Capture, then judge. Piping straight into grep discarded pg_restore's exit
# status, so a corrupt dump, a permission failure or a half-restored table all
# ended with "restore complete" as long as a documents table existed afterwards.
if ! docker exec -i "$PG_CONTAINER" pg_restore -U "$PGUSER" -d "$DB" --clean --if-exists --no-owner \
      < "$WORK/db.dump" > "$WORK/pg_restore.log" 2>&1; then
  grep -vE 'does not exist, skipping|errors ignored on restore' "$WORK/pg_restore.log" >&2 || true
  echo "pg_restore failed — the target database is NOT a usable restore" >&2
  exit 1
fi
grep -vE 'does not exist, skipping|errors ignored on restore' "$WORK/pg_restore.log" || true

echo "4/5  restoring blobs"
# The snapshot states its own format. Detecting it from what happens to be on
# this host routed every keys.txt snapshot to the packs the moment a pack INDEX
# existed, so a pool-era archive would fail against an index that never held its
# keys instead of reading the pool that does.
FORMAT=$(sed -n 's/.*"format"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$WORK/manifest.json" 2>/dev/null || true)
[ -n "$FORMAT" ] || FORMAT=$([ -d "$WORK/blobs" ] && echo legacy || echo pool-v2)
echo "     format: $FORMAT"

if [ -d "$WORK/blobs" ]; then
  # Legacy snapshot: the bytes travel inside the archive.
  echo "     legacy format (blobs embedded in archive)"
  if [ "${STORAGE_TYPE:-file}" = "file" ]; then
    mkdir -p "${BLOB_DIR:-./data/blobs}"
    cp -a "$WORK/blobs/." "${BLOB_DIR:-./data/blobs}/"
  else
    S3_BUCKET="$BUCKET" uv run --quiet scripts/s3util.py upload-blobs "$WORK/blobs"
  fi
elif [ "$FORMAT" = "pack-v1" ]; then
  # Ask the database we just restored what it references, rather than trusting
  # keys.txt: that file was queried after the dump was taken, so a hard delete in
  # between could drop a key the dump still needs. The restored database is the
  # authority on what this restore requires, and it cannot drift from itself.
  [ -f "$PACKS/INDEX" ] || { echo "pack index not found at $PACKS/INDEX" >&2; exit 1; }
  docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d "$DB" -tA -c \
    "select distinct content_key from (
       select content_key from documents where content_key is not null and content_key <> ''
       union all
       select content_key from document_revisions where content_key is not null and content_key <> ''
     ) k order by 1" > "$WORK/restore-keys.txt"
  echo "     pack format — $(wc -l < "$WORK/restore-keys.txt" | tr -d ' ') keys from $PACKS"
  # blobpack verifies every key is present and hash-correct before it writes
  # anything, and fails if it wrote fewer than asked.
  S3_BUCKET="$BUCKET" uv run --quiet scripts/blobpack.py emit "$PACKS" "$WORK/restore-keys.txt"

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
