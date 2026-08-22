#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["boto3"]
# ///
"""Blob packs: the backup's copy of the content store, as a few files.

Blobs are immutable and named by their own sha256, so they are collected into
pack files rather than stored one-per-file. Packs from previous days are sealed:
they never change again, so they are uploaded exactly once and read only during a
restore. Only the current day's pack is ever rewritten.

Storing blobs individually costs more in per-file block overhead than the data
occupies, and forces a directory that grows without bound — which in turn forces
sharding, incremental cursors, per-key presence checks and a garbage-collection
keep-set. Packing removes all of that: "what do I already have" is one index
file, not a walk of a directory with millions of entries.

  blobpack.py fold    <packsdir> [--all]   copy new content-store blobs into a pack
  blobpack.py emit    <packsdir> <keyfile> write those keys to the content store (restore)
  blobpack.py verify  <packsdir> [--packs a,b]  re-hash every blob and cross-check the index
  blobpack.py reindex <packsdir>           rebuild INDEX by scanning the pack files
  blobpack.py push    <packsdir>           mirror packs offsite (sealed ones once)
  blobpack.py pull    <packsdir> [--packs a,b]  fetch packs from offsite

`fold` reads keys from stdin, or with --all enumerates the whole content store.
--all is what backup.sh uses: a key list queried from the database races
hard-deletes, and folding a superset can only ever be wasteful, never wrong.

Durability rules this file obeys, because a backup that cannot restore is worse
than no backup:
  - A pack file is only ever created by writing a temp file and renaming it over
    the target. Nothing appends in place, so an interrupted run can never leave a
    half-written tar that the next run cannot open.
  - A pack is rewritten from the union of the members it already holds and the
    new blobs, keyed by name. That makes a retry after any crash idempotent and
    self-healing rather than duplicate-producing.
  - The index is ONE self-checking file, written atomically. There is no window
    in which a checksum and the data it covers can disagree.
  - Pack data is durable before the index that references it, locally and offsite.
"""
import hashlib
import io
import os
import sys
import tarfile
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

PACK_MAX = int(os.environ.get("BACKUP_PACK_MAX_BYTES", 64 * 1024 * 1024))
PAR = int(os.environ.get("BACKUP_PARALLEL", "16"))
INDEX_MAGIC = "#blobpack-index-v2"
FETCH_BATCH = int(os.environ.get("BACKUP_FETCH_BATCH", "256"))


def _s3(prefix):
    ep = os.environ[f"{prefix}S3_ENDPOINT"]
    if not ep.startswith("http"):
        ep = ("https://" if os.environ.get("S3_USE_SSL") == "true" else "http://") + ep
    return boto3.client("s3", endpoint_url=ep,
                        aws_access_key_id=os.environ[f"{prefix}S3_ACCESS_KEY"],
                        aws_secret_access_key=os.environ[f"{prefix}S3_SECRET_KEY"],
                        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"))


def backup_store():
    if not os.environ.get("BACKUP_S3_ENDPOINT"):
        sys.exit("BACKUP_S3_* must be set for offsite pack operations")
    return _s3("BACKUP_"), os.environ["BACKUP_S3_BUCKET"]


# --- content store backends ------------------------------------------------
# tracker runs on either a local directory or S3, chosen by STORAGE_TYPE. The
# backup has to speak whichever one this instance actually uses; assuming S3
# makes the default `.env` (STORAGE_TYPE=file) unbackupable, and — worse — can
# restore blobs into a bucket while the instance reads from a directory.

class FileBackend:
    kind = "file"

    def __init__(self, root):
        self.root = root

    def list_keys(self):
        out = []
        for dirpath, _, files in os.walk(self.root):
            for f in files:
                out.append(os.path.relpath(os.path.join(dirpath, f), self.root))
        return out

    def get(self, key):
        with open(os.path.join(self.root, key), "rb") as f:
            return f.read()

    def put(self, key, data):
        p = os.path.join(self.root, key)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        tmp = p + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)

    def prepare(self):
        os.makedirs(self.root, exist_ok=True)


class S3Backend:
    kind = "s3"

    def __init__(self, client, bucket):
        self.s3, self.bucket = client, bucket

    def list_keys(self):
        out = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket):
            for o in page.get("Contents", []):
                out.append(o["Key"])
        return out

    def get(self, key):
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def put(self, key, data):
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def prepare(self):
        if not any(b["Name"] == self.bucket for b in self.s3.list_buckets().get("Buckets", [])):
            self.s3.create_bucket(Bucket=self.bucket)


def content_backend():
    if os.environ.get("STORAGE_TYPE", "file") == "file":
        return FileBackend(os.environ.get("BLOB_DIR", "./data/blobs"))
    for v in ("S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_BUCKET"):
        if not os.environ.get(v):
            sys.exit(f"STORAGE_TYPE=s3 but {v} is not set")
    return S3Backend(_s3(""), os.environ["S3_BUCKET"])


# --- index -----------------------------------------------------------------
# One self-checking file. The checksum lives in the file it covers, so there is
# no window where an index and a separate checksum object disagree; a torn write
# never replaces the previous good copy, because writes go temp-then-rename.

def index_path(packs):
    return os.path.join(packs, "INDEX")


def _parse_index(raw):
    text = raw.decode()
    first, _, rest = text.partition("\n")
    if first.startswith(INDEX_MAGIC):
        want = first.split("sha256=", 1)[1].strip()
        got = hashlib.sha256(rest.encode()).hexdigest()
        if want != got:
            sys.exit(f"INDEX checksum mismatch (want {want[:12]}…, got {got[:12]}…) — refusing to use it")
        body = rest
    else:
        body = text  # legacy v1 index; checksum lived alongside, verified by caller
    idx = {}
    for line in body.splitlines():
        k, _, name = line.strip().partition(" ")
        if k:
            idx[k] = name
    return idx


def read_index(packs):
    p = index_path(packs)
    if not os.path.exists(p):
        return {}
    with open(p, "rb") as f:
        raw = f.read()
    legacy_sum = p + ".sha256"
    if not raw.startswith(INDEX_MAGIC.encode()) and os.path.exists(legacy_sum):
        with open(legacy_sum) as f:
            want = f.read().split()[0]
        if hashlib.sha256(raw).hexdigest() != want:
            sys.exit("legacy INDEX checksum mismatch — run `blobpack.py reindex` to rebuild")
    return _parse_index(raw)


def write_index(packs, idx):
    body = "".join(f"{k} {idx[k]}\n" for k in sorted(idx))
    raw = (f"{INDEX_MAGIC} sha256={hashlib.sha256(body.encode()).hexdigest()}\n" + body).encode()
    p = index_path(packs)
    tmp = p + ".tmp"
    with open(tmp, "wb") as f:
        f.write(raw)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)
    _fsync_dir(packs)
    # A legacy sidecar left by an older run would be stale and misleading.
    if os.path.exists(p + ".sha256"):
        os.remove(p + ".sha256")


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _today():
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _check(key, data):
    return not key.startswith("sha256/") or hashlib.sha256(data).hexdigest() == key.split("/", 1)[1]


def _framed(n):
    """Bytes a member of n payload bytes actually costs in a tar: a 512-byte
    header plus the payload padded up to the next 512-byte block. Counting raw
    payload alone made BACKUP_PACK_MAX_BYTES meaningless for small blobs, where
    framing is most of the file."""
    return 512 + ((n + 511) // 512) * 512


def _pack_members(path):
    """name -> bytes for an existing pack, or {} if it is not there."""
    if not os.path.exists(path):
        return {}
    out = {}
    with tarfile.open(path) as tf:
        for m in tf:
            if m.isfile():
                out[m.name] = tf.extractfile(m).read()
    return out


def _write_pack(path, members):
    """Create/replace a pack atomically. Never appends, so a kill mid-write
    leaves the previous pack (or no pack) rather than an unopenable tar."""
    tmp = path + ".tmp"
    with open(tmp, "wb") as raw:
        with tarfile.open(fileobj=raw, mode="w") as tf:
            for name in sorted(members):
                data = members[name]
                ti = tarfile.TarInfo(name)
                ti.size = len(data)
                tf.addfile(ti, io.BytesIO(data))
        raw.flush()
        os.fsync(raw.fileno())
    os.replace(tmp, path)
    _fsync_dir(os.path.dirname(path) or ".")


# --- commands --------------------------------------------------------------

def fold(packs, keys):
    os.makedirs(packs, exist_ok=True)
    idx = read_index(packs)
    missing = sorted(set(keys) - idx.keys())
    if not missing:
        if not os.path.exists(index_path(packs)):
            write_index(packs, idx)  # first run with nothing to store still needs an index
        print(f"packs: 0 new, {len(idx)} held across {len(set(idx.values()))} pack(s)")
        return

    backend = content_backend()
    today = _today()
    same_day = sorted(n for n in set(idx.values()) if n.startswith(f"blobs-{today}-"))
    # Only the current day's last pack is open for rewriting; earlier ones are sealed.
    if same_day and os.path.getsize(os.path.join(packs, same_day[-1])) < PACK_MAX:
        name = same_day[-1]
        members = _pack_members(os.path.join(packs, name))
        seq = int(name.rsplit("-", 1)[1].split(".")[0])
    else:
        seq = (int(same_day[-1].rsplit("-", 1)[1].split(".")[0]) + 1) if same_day else 1
        name = f"blobs-{today}-{seq:03d}.tar"
        members = {}

    added = 0
    size = sum(_framed(len(v)) for v in members.values())
    for i in range(0, len(missing), FETCH_BATCH):
        batch = missing[i:i + FETCH_BATCH]
        with ThreadPoolExecutor(max_workers=PAR) as ex:
            fetched = list(ex.map(lambda k: (k, backend.get(k)), batch))
        for k, data in fetched:
            if not _check(k, data):
                sys.exit(f"content store blob {k} does not match its own hash — refusing to pack it")
            # Roll over BEFORE the blob that would burst the cap, not after.
            if members and size + _framed(len(data)) > PACK_MAX:
                _write_pack(os.path.join(packs, name), members)
                seq += 1
                name = f"blobs-{today}-{seq:03d}.tar"
                members, size = {}, 0
            members[k] = data
            size += _framed(len(data))
            idx[k] = name
            added += 1

    _write_pack(os.path.join(packs, name), members)
    # Index last: it may only ever name packs whose bytes are already durable.
    write_index(packs, idx)
    sealed = sum(1 for n in set(idx.values()) if not n.startswith(f"blobs-{today}-"))
    print(f"packs: {added} new into {name}, {len(idx)} held across "
          f"{len(set(idx.values()))} pack(s) ({sealed} sealed) [{backend.kind}]")


def verify(packs, only=None):
    idx = read_index(packs)
    names = sorted(only or set(idx.values()))
    bad = n = 0
    seen = set()
    for name in names:
        p = os.path.join(packs, name)
        if not os.path.exists(p):
            print(f"  MISSING pack {name}")
            bad += 1
            continue
        with tarfile.open(p) as tf:
            for m in tf:
                data = tf.extractfile(m).read()
                n += 1
                seen.add(m.name)
                if not _check(m.name, data):
                    print(f"  CORRUPT {m.name} in {name}")
                    bad += 1
                if idx.get(m.name) not in (None, name):
                    print(f"  MISPLACED {m.name}: index says {idx[m.name]}, found in {name}")
                    bad += 1
    # The index promising a blob that no pack holds is the failure that makes a
    # restore die halfway, so check that direction too.
    if only is None:
        for k, name in idx.items():
            if k not in seen:
                print(f"  ORPHAN INDEX ENTRY {k} -> {name} (no such member)")
                bad += 1
    print(f"verify: {n} blobs across {len(names)} pack(s), {bad} problem(s)")
    return 1 if bad else 0


def reindex(packs):
    """Rebuild INDEX from the packs themselves. Member names are the keys, so a
    lost or torn index is recoverable as long as the pack bytes survive."""
    idx, n, bad = {}, 0, 0
    names = sorted(f for f in os.listdir(packs) if f.startswith("blobs-") and f.endswith(".tar"))
    for name in names:
        with tarfile.open(os.path.join(packs, name)) as tf:
            for m in tf:
                if not m.isfile():
                    continue
                data = tf.extractfile(m).read()
                n += 1
                if not _check(m.name, data):
                    print(f"  CORRUPT {m.name} in {name} — not indexed")
                    bad += 1
                    continue
                idx[m.name] = name  # duplicates are byte-identical: the key IS the hash
    write_index(packs, idx)
    print(f"reindex: {len(idx)} keys from {n} member(s) across {len(names)} pack(s), {bad} rejected")
    return 1 if bad else 0


def emit(packs, keyfile):
    """Restore path: write exactly these keys to the content store.

    Two passes on purpose. The first proves every requested key is present and
    hash-correct; only then does the second write anything. A restore that
    discovers a missing blob halfway through has already corrupted the target.
    """
    keys = {k.strip() for k in open(keyfile) if k.strip()}
    idx = read_index(packs)
    absent = sorted(keys - idx.keys())
    if absent:
        sys.exit(f"packs cannot satisfy this restore: {len(absent)} key(s) not in any pack "
                 f"(e.g. {absent[0]}) — refusing a partial restore")
    by_pack = {}
    for k in keys:
        by_pack.setdefault(idx[k], set()).add(k)
    gone = [n for n in by_pack if not os.path.exists(os.path.join(packs, n))]
    if gone:
        sys.exit(f"cannot restore: pack file(s) absent: {', '.join(sorted(gone))}")

    found = set()
    for name, want in sorted(by_pack.items()):
        with tarfile.open(os.path.join(packs, name)) as tf:
            for m in tf:
                if m.name in want:
                    if not _check(m.name, tf.extractfile(m).read()):
                        sys.exit(f"pack {name} holds a corrupt blob for {m.name} — refusing to restore corruption")
                    found.add(m.name)
    short = sorted(keys - found)
    if short:
        sys.exit(f"packs are indexed for {len(short)} key(s) whose bytes are not in the pack "
                 f"(e.g. {short[0]}) — refusing a partial restore; try `blobpack.py reindex`")

    backend = content_backend()
    backend.prepare()
    sent = 0
    for name, want in sorted(by_pack.items()):
        with tarfile.open(os.path.join(packs, name)) as tf:
            batch = [(m.name, tf.extractfile(m).read()) for m in tf if m.name in want]
        with ThreadPoolExecutor(max_workers=PAR) as ex:
            list(ex.map(lambda t: backend.put(t[0], t[1]), batch))
        sent += len(batch)
    if sent != len(keys):
        sys.exit(f"restore wrote {sent} of {len(keys)} blobs — treat this restore as failed")
    print(f"restored {sent} blobs from {len(by_pack)} pack(s) to {backend.kind}")


def offsite_prefix():
    base = os.environ.get("BACKUP_S3_PREFIX", "").strip("/")
    return (base + "/packs/").lstrip("/")


def push(packs):
    """Mirror packs offsite. Sealed packs are uploaded once and never again."""
    s3, bucket = backup_store()
    if not any(b["Name"] == bucket for b in s3.list_buckets().get("Buckets", [])):
        s3.create_bucket(Bucket=bucket)
    pre = offsite_prefix()
    have = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=pre):
        for o in page.get("Contents", []):
            have[o["Key"][len(pre):]] = o["Size"]
    idx = read_index(packs)
    today = _today()
    sent = skipped = 0
    for name in sorted(set(idx.values())):
        p = os.path.join(packs, name)
        size = os.path.getsize(p)
        if have.get(name) == size and not name.startswith(f"blobs-{today}-"):
            skipped += 1
            continue
        s3.upload_file(p, bucket, pre + name)
        sent += 1
    # Index last, for the same reason it is written last locally: an offsite
    # index must never reference pack bytes that are not offsite yet.
    s3.upload_file(index_path(packs), bucket, pre + "INDEX")
    print(f"pack push: {sent} uploaded, {skipped} sealed pack(s) already offsite")


def pull(packs, only=None):
    s3, bucket = backup_store()
    pre = offsite_prefix()
    os.makedirs(packs, exist_ok=True)
    todo = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=pre):
        for o in page.get("Contents", []):
            rel = o["Key"][len(pre):]
            if rel == "INDEX":
                continue  # always refetched below; size is not an identity check
            if only and rel not in set(only):
                continue
            dst = os.path.join(packs, rel)
            if os.path.exists(dst) and os.path.getsize(dst) == o["Size"]:
                continue
            todo.append((o["Key"], dst))
    with ThreadPoolExecutor(max_workers=PAR) as ex:
        list(ex.map(lambda t: s3.download_file(bucket, t[0], t[1]), todo))
    # Fetch the index to a temp path and parse (which verifies it) before it is
    # allowed to replace a copy that currently works.
    tmp = index_path(packs) + ".fetch"
    s3.download_file(bucket, pre + "INDEX", tmp)
    with open(tmp, "rb") as f:
        _parse_index(f.read())
    os.replace(tmp, index_path(packs))
    _fsync_dir(packs)
    if os.path.exists(index_path(packs) + ".sha256"):
        os.remove(index_path(packs) + ".sha256")
    print(f"pack pull: {len(todo)} pack(s) + INDEX fetched into {packs}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    cmd, packs = sys.argv[1], sys.argv[2]
    rest = sys.argv[3:]
    only = None
    if "--packs" in rest:
        only = rest[rest.index("--packs") + 1].split(",")
    if cmd == "fold":
        if "--all" in rest:
            fold(packs, content_backend().list_keys())
        else:
            fold(packs, [l.strip() for l in sys.stdin if l.strip()])
    elif cmd == "emit":
        emit(packs, rest[0])
    elif cmd == "verify":
        sys.exit(verify(packs, only))
    elif cmd == "reindex":
        sys.exit(reindex(packs))
    elif cmd == "push":
        push(packs)
    elif cmd == "pull":
        pull(packs, only)
    else:
        sys.exit(f"unknown command {cmd}")
