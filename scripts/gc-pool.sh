#!/usr/bin/env bash
# Drop blobs from the backup pool that nothing can still need.
#
#   scripts/gc-pool.sh            # report only (default)
#   scripts/gc-pool.sh --apply    # actually delete
#
# The pool is shared by every retained snapshot, so "unreferenced" cannot mean
# "unreferenced by the live database". A blob whose document was hard-deleted
# yesterday is still needed by every snapshot taken before that. Deleting on the
# live view alone silently guts older restores — the failure only shows up the
# day you try to use one.
#
# So the keep set is the union of:
#   - every content_key in the live database (docs + revisions), and
#   - every key listed in keys.txt of every retained snapshot.
#
# Anything else is unreachable: no snapshot names it and no live row points at
# it. Those are the true orphans (hard-deleted content, aborted writes).
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

PG_CONTAINER=${PG_CONTAINER:-tracker-postgres}
OUT_DIR=${BACKUP_DIR:-./backups}
POOL=${BACKUP_POOL_DIR:-$OUT_DIR/pool}
APPLY=0
[ "${1:-}" = "--apply" ] && APPLY=1

[ -d "$POOL" ] || { echo "no pool at $POOL"; exit 1; }
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT

# 1) keys the live database still points at
docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDATABASE" -tA -c \
  "select distinct content_key from (
     select content_key from documents where content_key is not null and content_key <> ''
     union all
     select content_key from document_revisions where content_key is not null and content_key <> ''
   ) k" > "$WORK/keep.raw"
LIVE=$(wc -l < "$WORK/keep.raw" | tr -d ' ')

# 2) keys every retained snapshot names. A snapshot with no keys.txt is a
#    legacy tarball that carries its own blobs, so it needs nothing from us.
SNAPS=0
for t in "$OUT_DIR"/tracker-backup-*.tar.gz; do
  [ -f "$t" ] || continue
  # Read the listing into a variable first. Piping tar into `grep -q` under
  # `set -o pipefail` makes grep exit on the first match, tar die of SIGPIPE,
  # and the pipeline report failure — which would silently skip every snapshot
  # and shrink the keep set to the live database alone.
  listing=$(tar tzf "$t" 2>/dev/null || true)
  if grep -qx keys.txt <<<"$listing"; then
    tar xzf "$t" -C "$WORK" keys.txt 2>/dev/null
    cat "$WORK/keys.txt" >> "$WORK/keep.raw"
    SNAPS=$((SNAPS+1))
  fi
done

sort -u "$WORK/keep.raw" > "$WORK/keep.txt"
KEEP=$(wc -l < "$WORK/keep.txt" | tr -d ' ')
POOL_N=$(find "$POOL" -type f | wc -l | tr -d ' ')
ORPHANS=$((POOL_N - KEEP))

echo "pool:      $POOL_N blobs"
echo "live db:   $LIVE keys"
echo "snapshots: $SNAPS with a key list"
echo "keep set:  $KEEP (union)"
echo "orphans:   $ORPHANS"

if [ "$APPLY" = 1 ]; then
  uv run --quiet scripts/s3util.py gc-pool "$POOL" "$WORK/keep.txt"
else
  echo
  echo "(report only — rerun with --apply to delete)"
fi
