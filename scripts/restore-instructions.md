# tracker — restoring from this bucket

You are reading this because the tracker instance is gone and this bucket is
what is left. Everything needed to rebuild is here; nothing in this file is a
secret.

<!-- FACTS -->

## What you need

- **Read credentials for this bucket.** The only secret in the process. If you
  are reading this file you already have them.
- **Docker**, or a Postgres 17 (with pgvector) and somewhere S3-compatible to
  put blobs.
- Disk for the database dump plus the blob pool (sizes in the facts above).

The tracker image and source are both public — no GitHub account required:

    docker pull ghcr.io/chicagobuss/tracker:latest
    git clone https://github.com/chicagobuss/tracker

## What is in the bucket

    <prefix>/tracker-backup-<timestamp>.tar.gz   point-in-time snapshots
    <prefix>/packs/blobs-<YYYYMMDD>-NNN.tar      content blobs, shared by all snapshots
    <prefix>/packs/INDEX                         which pack holds which blob (+ .sha256)
    <prefix>/RESTORE.md                          this file

A snapshot is small and contains no blob bytes:

    db.dump         Postgres dump, custom format (pg_restore)
    keys.txt        the content keys this snapshot needs, one per line
    manifest.json   counts, versions, storage type, created_at

Blobs are immutable and named by their own sha256, so the packs serve every
snapshot. **A snapshot alone is not a restore — you need the packs too.** A pack
for a past day is sealed and never changes; only the current day's is rewritten.

Older layouts still restore: snapshots that reference `<prefix>/pool/sha256/...`
(per-file blobs) or that embed their own `blobs/` directory are detected
automatically.

## The easy path (with the repo)

    git clone https://github.com/chicagobuss/tracker && cd tracker
    cp .env.example .env      # then set the variables named below

Set in `.env`: `BACKUP_S3_ENDPOINT`, `BACKUP_S3_BUCKET`, `BACKUP_S3_PREFIX`,
`BACKUP_S3_ACCESS_KEY`, `BACKUP_S3_SECRET_KEY` for reading this bucket, and
`S3_*` (or `STORAGE_TYPE=file`) for wherever blobs should now live.

    scripts/s3util.py list-archives                    # pick a snapshot
    scripts/blobpack.py pull ./backups                 # fetch the packs
    scripts/restore.sh --from-s3 tracker-backup-<ts>.tar.gz

Then start the service with `.env` pointing at that database and blob store.

## The bare path (no repo, no scripts)

1. Download a snapshot and extract it: `db.dump`, `keys.txt`, `manifest.json`.
2. Download `<prefix>/packs/` — the `.tar` files plus `INDEX`. Each pack is an
   ordinary tar whose member names are the blob keys, so `tar xf` works with no
   special tooling.
3. Create a Postgres database and restore into it:

       pg_restore -U <user> -d <db> --clean --if-exists --no-owner db.dump

4. Extract the packs and put the blob bytes where the new instance will look for
   them: upload each under the *same* `sha256/<hash>` key it has inside the tar,
   or copy them into `BLOB_DIR` with `STORAGE_TYPE=file`.
5. Clear stale write-leases: `delete from doc_locks;`
6. Run the image, pointing `DATABASE_URL` and the `S3_*`/`BLOB_DIR` settings at
   what you just built. Migrations run automatically on startup and are
   idempotent.

## Verify before trusting it

- `GET /healthz` returns `{"status":"ok","version":...}`.
- Document count matches `documents` in `manifest.json`.
- Blobs are self-verifying: the sha256 of each blob's bytes must equal the hash
  in its key. Any mismatch is corruption, not a restore artifact.
- Fetch a document's raw content and confirm it is not empty — that exercises
  the database and the blob store together, which nothing else does.

## Things that will confuse you otherwise

- **Workspaces** partition the store (`default` holds anything predating them).
  A restored instance defaults to `DEFAULT_WORKSPACE`; other workspaces are
  present but you must ask for them with `?workspace=` or `X-Workspace`.
- **Auth is off** when `API_TOKENS` is empty. Fine on a private network; set it
  before exposing the instance anywhere else.
- **Blob content URLs** are built from `BASE_URL`, so set that to whatever
  address clients will actually use.
- Archives in this bucket are **never pruned**; local retention (`BACKUP_KEEP`)
  does not apply here. The newest is not necessarily the one you want — check
  `created_at` in `manifest.json`.
