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
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

PG_CONTAINER=${PG_CONTAINER:-coord-postgres}
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
if [ "${STORAGE_TYPE:-file}" = "file" ]; then
  echo "     copying to local directory ${BLOB_DIR:-./data/blobs}"
  mkdir -p "${BLOB_DIR:-./data/blobs}"
  cp -a "$WORK/blobs/." "${BLOB_DIR:-./data/blobs}/"
else
  echo "     upload blobs -> $BUCKET"
  S3_BUCKET="$BUCKET" uv run --quiet scripts/s3util.py upload-blobs "$WORK/blobs"
fi

echo "5/5  clear stale leases"
docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d "$DB" -tAc "delete from doc_locks;" >/dev/null 2>&1 || true

DOCS=$(docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d "$DB" -tAc "select count(*) from documents")
echo "restore complete: db=$DB ($DOCS docs), storage=${STORAGE_TYPE:-file}"
echo "  -> start the service: docker compose up -d tracker (with .env pointing at this db/storage)"
