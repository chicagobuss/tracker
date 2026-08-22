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
  blobpack.py coverage <packsdir> [keys]   check every indexed pack is present and intact
  blobpack.py push    <packsdir> [--gc]    mirror packs offsite (sealed ones once)
  blobpack.py gc-offsite <packsdir>        delete offsite packs the offsite index does not name
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
import contextlib
import fcntl
import hashlib
import io
import os
import re
import sys
import tarfile
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

PACK_MAX = int(os.environ.get("BACKUP_PACK_MAX_BYTES", 64 * 1024 * 1024))
PAR = int(os.environ.get("BACKUP_PARALLEL", "16"))
INDEX_MAGIC = "#blobpack-index-v3"
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
    """-> (key -> pack, pack -> (sha256, size)).

    The pack digests are what let push and pull decide identity by content. Size
    alone cannot: a pack rewritten with different bytes but the same length reads
    as "already there", and the index then names bytes that are not present.
    """
    text = raw.decode()
    first, _, rest = text.partition("\n")
    if first.startswith("#blobpack-index-v"):
        want = first.split("sha256=", 1)[1].strip()
        got = hashlib.sha256(rest.encode()).hexdigest()
        if want != got:
            sys.exit(f"INDEX checksum mismatch (want {want[:12]}…, got {got[:12]}…) — refusing to use it")
        body = rest
    else:
        body = text  # legacy v1 index; checksum lived alongside, verified by caller
    idx, meta = {}, {}
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#pack "):
            _, name, digest, size = line.split()
            meta[name] = (digest, int(size))
            continue
        if line.startswith("#"):
            continue
        k, _, name = line.partition(" ")
        if k:
            idx[k] = name
    return idx, meta


def read_index(packs):
    p = index_path(packs)
    if not os.path.exists(p):
        return {}, {}
    with open(p, "rb") as f:
        raw = f.read()
    legacy_sum = p + ".sha256"
    if not raw.startswith(b"#blobpack-index-v") and os.path.exists(legacy_sum):
        with open(legacy_sum) as f:
            want = f.read().split()[0]
        if hashlib.sha256(raw).hexdigest() != want:
            sys.exit("legacy INDEX checksum mismatch — run `blobpack.py reindex` to rebuild")
    return _parse_index(raw)


def _digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest(), os.path.getsize(path)


def write_index(packs, idx, meta=None):
    if meta is None:
        meta = {n: _digest(os.path.join(packs, n)) for n in sorted(set(idx.values()))}
    body = "".join(f"#pack {n} {meta[n][0]} {meta[n][1]}\n" for n in sorted(meta))
    body += "".join(f"{k} {idx[k]}\n" for k in sorted(idx))
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
    """name -> bytes for an existing pack, or {} if it is not there.

    Every member is re-hashed on the way in. This is the inductive step that
    keeps a generation trustworthy: the open pack is the only old data a fold
    ever copies forward, so validating it here means a new generation can only
    contain bytes that were correct when it was built. Without it, a payload bit
    that rotted on disk was silently repacked into the new pack, whose whole-file
    digest then matched the new index perfectly — so coverage passed, push
    succeeded, and the corruption was only discovered by a restore.
    """
    if not os.path.exists(path):
        return {}
    out = {}
    with tarfile.open(path) as tf:
        for m in tf:
            if not m.isfile():
                continue
            data = tf.extractfile(m).read()
            if not _check(m.name, data):
                sys.exit(f"{os.path.basename(path)} holds a corrupt blob for {m.name} — refusing to "
                         f"carry it into a new pack. Restore that pack from offsite (`blobpack.py pull`) "
                         f"or rebuild the index (`blobpack.py reindex`) before backing up again.")
            out[m.name] = data
    return out


PACK_RE = re.compile(r"^blobs-(\d{8})-(\d{3})(?:-([0-9a-f]{12}|[0-9a-f]{64}))?\.tar$")


def _write_pack(packs, day, seq, members):
    """Write a pack under a name that includes its own digest, and return it.

    Pack names are content-addressed on purpose. When the day's open pack gains
    blobs it is not rewritten in place — it is published under a new name, so the
    bytes the currently committed index points at are never modified. That makes
    publishing a generation a single index write, locally and offsite: an
    interrupted push, pull or fold leaves the previous index still pointing at
    packs that still exist and still hash correctly. With a mutable pathname,
    overwriting the pack before the index landed destroyed the committed
    generation, and two writers could interleave into a readable index naming
    bytes nobody wrote.
    """
    tmp = os.path.join(packs, f".pack-{day}-{seq:03d}.tmp")
    with open(tmp, "wb") as raw:
        with tarfile.open(fileobj=raw, mode="w") as tf:
            for name in sorted(members):
                data = members[name]
                ti = tarfile.TarInfo(name)
                ti.size = len(data)
                tf.addfile(ti, io.BytesIO(data))
        raw.flush()
        os.fsync(raw.fileno())
    digest, size = _digest(tmp)
    # The FULL digest, not a prefix. A truncated one is not an identity: two
    # different foldings sharing 48 bits resolve to the same pathname, and the
    # second write then overwrites the pack the committed index still names.
    name = f"blobs-{day}-{seq:03d}-{digest}.tar"
    os.replace(tmp, os.path.join(packs, name))
    _fsync_dir(packs)
    return name, digest, size


def _prune(packs, keep):
    """Remove pack files no longer named by the committed index. Safe only after
    the index that supersedes them is durable."""
    gone = 0
    for f in os.listdir(packs):
        if PACK_RE.match(f) and f not in keep:
            os.remove(os.path.join(packs, f))
            gone += 1
    if gone:
        _fsync_dir(packs)
    return gone


# --- commands --------------------------------------------------------------

def fold(packs, keys):
    os.makedirs(packs, exist_ok=True)
    # Two folds against one packs directory would each read the index, each
    # rewrite a pack, and the second would publish an index with no knowledge of
    # the first — silently dropping the earlier run's keys. backup.sh has its own
    # flock, but it is per-host and does not cover a hand-run fold, so hold an
    # exclusive lock on the packs directory itself for the whole read-modify-write.
    with _lock(packs, fcntl.LOCK_EX):
        _fold_locked(packs, keys)


@contextlib.contextmanager
def _lock(packs, mode):
    """Serialise pack-directory access. fold takes LOCK_EX; every reader takes
    LOCK_SH, because "prune after the index is durable" only protects readers
    that start after the new index — a push or emit already holding the previous
    generation would have had its packs deleted underneath it."""
    os.makedirs(packs, exist_ok=True)
    with open(os.path.join(packs, ".lock"), "w") as lk:
        fcntl.flock(lk, mode)
        try:
            yield
        finally:
            fcntl.flock(lk, fcntl.LOCK_UN)


def _fold_locked(packs, keys):
    idx, meta = read_index(packs)
    # Test-only fault injection. Serialisation bugs here are timing-dependent, so
    # a test that merely launches two folds at once usually sees them run one
    # after another and passes whether or not the lock exists. This widens the
    # read-modify-write window on demand so the test can prove the lock is doing
    # the work. Unset in every real run.
    if os.environ.get("BLOBPACK_TEST_PAUSE"):
        time.sleep(float(os.environ["BLOBPACK_TEST_PAUSE"]))
    missing = sorted(set(keys) - idx.keys())
    if not missing:
        if not os.path.exists(index_path(packs)):
            write_index(packs, idx, meta)  # first run with nothing to store still needs an index
        print(f"packs: 0 new, {len(idx)} held across {len(set(idx.values()))} pack(s)")
        return

    backend = content_backend()
    day = _today()
    # The open pack is the highest-sequence pack of today that is still under the
    # cap; everything else is sealed and is never read or rewritten again.
    today = []
    for n in set(idx.values()):
        m = PACK_RE.match(n)
        if m and m.group(1) == day:
            today.append((int(m.group(2)), n))
    today.sort()
    members, seq, replacing = {}, 1, None
    if today:
        seq, last = today[-1]
        if os.path.getsize(os.path.join(packs, last)) < PACK_MAX:
            members = _pack_members(os.path.join(packs, last))
            replacing = last
        else:
            seq += 1

    added, published = 0, {}
    size = sum(_framed(len(v)) for v in members.values())
    for i in range(0, len(missing), FETCH_BATCH):
        batch = missing[i:i + FETCH_BATCH]
        with ThreadPoolExecutor(max_workers=PAR) as ex:
            fetched = list(ex.map(lambda k: (k, backend.get(k)), batch))
        for k, data in fetched:
            if not _check(k, data):
                sys.exit(f"content store blob {k} does not match its own hash — refusing to pack it")
            if members and size + _framed(len(data)) > PACK_MAX:
                name, digest, psize = _write_pack(packs, day, seq, members)
                published[name] = (digest, psize, set(members))
                seq += 1
                members, size = {}, 0
            members[k] = data
            size += _framed(len(data))
            added += 1

    name, digest, psize = _write_pack(packs, day, seq, members)
    published[name] = (digest, psize, set(members))

    for name, (digest, psize, names) in published.items():
        meta[name] = (digest, psize)
        for k in names:
            idx[k] = name
    # Drop the superseded open pack from the index before publishing it.
    if replacing and replacing not in published:
        meta.pop(replacing, None)
    meta = {n: v for n, v in meta.items() if n in set(idx.values())}

    # Index last: it may only ever name packs whose bytes are already durable.
    write_index(packs, idx, meta)
    dropped = _prune(packs, set(idx.values()))
    sealed = sum(1 for n in set(idx.values()) if not n.startswith(f"blobs-{day}-"))
    print(f"packs: {added} new into {len(published)} pack(s), {len(idx)} held across "
          f"{len(set(idx.values()))} pack(s) ({sealed} sealed, {dropped} superseded removed) [{backend.kind}]")


def verify(packs, only=None):
    idx, meta = read_index(packs)
    names = sorted(only or set(idx.values()))
    bad = n = 0
    seen = set()
    for name in names:
        p = os.path.join(packs, name)
        if name in meta and os.path.exists(p):
            digest, size = _digest(p)
            if (digest, size) != meta[name]:
                print(f"  PACK DIGEST MISMATCH {name}: index records {meta[name][0][:12]}…, file is {digest[:12]}…")
                bad += 1
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
                if m.name in seen:
                    # A legacy pack written by the original append implementation
                    # can hold the same member twice. Byte-identical, but emit
                    # must not be surprised by it and a snapshot must not be
                    # written against it silently.
                    print(f"  DUPLICATE MEMBER {m.name} in {name}")
                    bad += 1
                seen.add(m.name)
                if not _check(m.name, data):
                    print(f"  CORRUPT {m.name} in {name}")
                    bad += 1
                if idx.get(m.name) not in (None, name):
                    print(f"  MISPLACED {m.name}: index says {idx[m.name]}, found in {name}")
                    bad += 1
    # The index promising a blob that no pack holds is the failure that makes a
    # restore die halfway, so check that direction too. Under --packs this is
    # restricted to the selected packs rather than skipped: skipping it made a
    # filtered verify return success for an indexed-but-absent member.
    scope = set(names)
    for k, name in idx.items():
        if name in scope and k not in seen:
            print(f"  ORPHAN INDEX ENTRY {k} -> {name} (no such member)")
            bad += 1
    print(f"verify: {n} blobs across {len(names)} pack(s), {bad} problem(s)")
    return 1 if bad else 0


def reindex(packs):
    """Rebuild INDEX from the packs themselves. Member names are the keys, so a
    lost or torn index is recoverable as long as the pack bytes survive."""
    idx, n, bad, dupes = {}, 0, 0, 0
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
                if m.name in idx:
                    dupes += 1  # byte-identical by construction: the key IS the hash
                idx[m.name] = name
    write_index(packs, idx, {n_: _digest(os.path.join(packs, n_)) for n_ in sorted(set(idx.values()))})
    print(f"reindex: {len(idx)} keys from {n} member(s) across {len(names)} pack(s), "
          f"{bad} rejected, {dupes} duplicate member(s) collapsed")
    return 1 if bad else 0


def emit(packs, keyfile):
    """Restore path: write exactly these keys to the content store.

    Two passes on purpose. The first proves every requested key is present and
    hash-correct; only then does the second write anything. A restore that
    discovers a missing blob halfway through has already corrupted the target.
    """
    keys = {k.strip() for k in open(keyfile) if k.strip()}
    idx, _meta = read_index(packs)
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
            found_here = {}
            for m in tf:
                if m.name not in want or m.name in found_here:
                    continue  # a legacy pack may hold a member twice; take the first
                data = tf.extractfile(m).read()
                # Re-hash here, not only in the preflight pass. Trusting the
                # first pass means a pack replaced between the two passes can
                # put wrong bytes into the content store under a correct name.
                if not _check(m.name, data):
                    sys.exit(f"pack {name} changed under us: {m.name} no longer matches its hash — restore aborted")
                found_here[m.name] = data
            batch = sorted(found_here.items())
        if len(batch) != len(want):
            sys.exit(f"pack {name} lost {len(want) - len(batch)} member(s) between preflight and read — restore aborted")
        with ThreadPoolExecutor(max_workers=PAR) as ex:
            list(ex.map(lambda t: backend.put(t[0], t[1]), batch))
        sent += len(batch)
    if sent != len(keys):
        sys.exit(f"restore wrote {sent} of {len(keys)} blobs — treat this restore as failed")
    print(f"restored {sent} blobs from {len(by_pack)} pack(s) to {backend.kind}")


def offsite_prefix():
    base = os.environ.get("BACKUP_S3_PREFIX", "").strip("/")
    return (base + "/packs/").lstrip("/")


def push(packs, gc=False):
    """Mirror packs offsite, then publish the exact index those packs belong to.

    Two rules make this recoverable. First, push uploads the INDEX *bytes it
    read*, not whatever is at the pathname later — otherwise a fold committing
    mid-push publishes an index offsite whose packs push never selected or
    uploaded. Second, superseded remote packs are not deleted here: a delete
    decided from a listing taken before another pusher's upload can remove the
    winning generation's packs. Offsite GC is a separate, explicit operation.
    """
    s3, bucket = backup_store()
    if not any(b["Name"] == bucket for b in s3.list_buckets().get("Buckets", [])):
        s3.create_bucket(Bucket=bucket)
    pre = offsite_prefix()
    with _lock(packs, fcntl.LOCK_SH):
        with open(index_path(packs), "rb") as f:
            index_bytes = f.read()
        idx, meta = _parse_index(index_bytes)
        referenced = sorted(set(idx.values()))
        remote = {}
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=pre):
            for o in page.get("Contents", []):
                remote[o["Key"][len(pre):]] = o["Size"]
        sent = skipped = 0
        for name in referenced:
            path = os.path.join(packs, name)
            digest, size = meta.get(name) or _digest(path)
            if _digest(path) != (digest, size):
                sys.exit(f"local pack {name} does not match the digest the index records — refusing to publish it")
            if remote.get(name) == size and _remote_digest(s3, bucket, pre + name) == digest:
                skipped += 1
                continue
            s3.upload_file(path, bucket, pre + name, ExtraArgs={"Metadata": {"sha256": digest}})
            sent += 1
        # Publication: the index that describes exactly the packs just verified.
        s3.put_object(Bucket=bucket, Key=pre + "INDEX", Body=index_bytes)
    dropped = _gc_offsite(s3, bucket, pre, set(referenced)) if gc else 0
    tail = f", {dropped} superseded removed" if gc else ""
    print(f"pack push: {sent} uploaded, {skipped} already offsite{tail}")


def _remote_digest(s3, bucket, key):
    try:
        return s3.head_object(Bucket=bucket, Key=key).get("Metadata", {}).get("sha256")
    except s3.exceptions.ClientError:
        return None


def _gc_offsite(s3, bucket, pre, referenced=None):
    """Delete offsite packs the CURRENT offsite index does not name.

    Re-reads the index from the store immediately beforehand rather than trusting
    a listing taken earlier, so this cannot delete a generation another pusher
    committed in the meantime. Still assumes no push is running concurrently —
    which is why it is not part of push.
    """
    body = s3.get_object(Bucket=bucket, Key=pre + "INDEX")["Body"].read()
    idx, _ = _parse_index(body)
    live = set(idx.values()) | (referenced or set())
    dropped = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=pre):
        for o in page.get("Contents", []):
            rel = o["Key"][len(pre):]
            if rel != "INDEX" and PACK_RE.match(rel) and rel not in live:
                s3.delete_object(Bucket=bucket, Key=o["Key"])
                dropped += 1
    return dropped


def coverage(packs, keyfile=None):
    """Prove the packs can serve a snapshot before one is written against them.

    Verifies EVERY pack the index references, not merely the packs named by a key
    list. The key list is queried after the dump, so a delete racing the dump can
    shrink it to a set that omits the damaged pack entirely — and a coverage gate
    driven by that list then hashes nothing and passes. `fold --all` makes the
    index a superset of anything a dump can reference, so checking all of it is
    both the cheap option and the correct one.
    """
    idx, meta = read_index(packs)
    bad = []
    for n in sorted(set(idx.values())):
        path = os.path.join(packs, n)
        if not os.path.exists(path):
            bad.append(f"{n}: missing")
        elif n not in meta:
            bad.append(f"{n}: index records no digest for it — run `blobpack.py reindex`")
        elif _digest(path) != meta[n]:
            bad.append(f"{n}: does not match the digest the index records")
    if bad:
        sys.exit("refusing to write a snapshot against unusable packs:\n  " + "\n  ".join(bad))
    if keyfile:
        need = {l.strip() for l in open(keyfile) if l.strip()}
        absent = need - idx.keys()
        if absent:
            sys.exit(f"packs are missing {len(absent)} referenced blob(s) — aborting")
        print(f"     {len(need)} snapshot keys held; {len(set(idx.values()))} pack(s) present and verified")
    else:
        print(f"coverage: {len(idx)} keys across {len(set(idx.values()))} pack(s), all present and verified")


def gc_offsite(packs):
    # Exclusive lock: a push on this host may have uploaded a pack whose index is
    # not published yet, and GC reading the still-current index would see it as
    # unreferenced and delete it out from under the commit.
    s3, bucket = backup_store()
    with _lock(packs, fcntl.LOCK_EX):
        n = _gc_offsite(s3, bucket, offsite_prefix())
    print(f"offsite gc: {n} superseded pack(s) removed")


def pull(packs, only=None):
    """Fetch the candidate index, make every pack it names present and correct,
    and only then install it.

    The index is installed last and packs are never overwritten (their names carry
    their digest), so an interruption leaves the previous local generation intact
    rather than a new index over old bytes or an old index over new ones. A
    filtered pull refuses to install an index naming packs it did not fetch.
    """
    s3, bucket = backup_store()
    pre = offsite_prefix()
    os.makedirs(packs, exist_ok=True)
    tmp = index_path(packs) + ".fetch"
    s3.download_file(bucket, pre + "INDEX", tmp)
    with open(tmp, "rb") as f:
        idx, meta = _parse_index(f.read())

    referenced = set(idx.values())
    wanted = referenced & set(only) if only else referenced
    todo = []
    for name in sorted(wanted):
        dst = os.path.join(packs, name)
        if os.path.exists(dst) and name in meta and _digest(dst) == meta[name]:
            continue
        todo.append(name)

    def _get(name):
        dst = os.path.join(packs, name)
        part = dst + ".fetch"
        s3.download_file(bucket, pre + name, part)
        if name in meta and _digest(part) != meta[name]:
            os.remove(part)
            sys.exit(f"pack {name} fetched from offsite does not match the digest the index records — aborting")
        # fsync the contents before the name exists, or a crash can leave a
        # durable filename over data that never reached the disk.
        fd = os.open(part, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(part, dst)

    with ThreadPoolExecutor(max_workers=PAR) as ex:
        list(ex.map(_get, todo))
    _fsync_dir(packs)

    # Existence is not satisfaction: a filtered pull only digest-checks what it
    # fetched, so a pack already present but corrupt would let the guard pass.
    unsatisfied = []
    for n in sorted(referenced):
        path = os.path.join(packs, n)
        if not os.path.exists(path) or (n in meta and _digest(path) != meta[n]):
            unsatisfied.append(n)
    if unsatisfied:
        os.remove(tmp)
        sys.exit(f"refusing to install an index naming {len(unsatisfied)} pack(s) missing or "
                 f"mismatched here (e.g. {unsatisfied[0]}) — run without --packs for a complete generation")
    # The candidate index must be durable before it replaces a working one, or a
    # power loss leaves a durable name over bytes that never reached the disk.
    fd = os.open(tmp, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, index_path(packs))
    _fsync_dir(packs)
    if os.path.exists(index_path(packs) + ".sha256"):
        os.remove(index_path(packs) + ".sha256")
    print(f"pack pull: {len(todo)} pack(s) fetched, INDEX installed in {packs}")


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
        # Shared lock: a fold committing mid-restore would otherwise prune the
        # pack this emit is part-way through reading.
        with _lock(packs, fcntl.LOCK_SH):
            emit(packs, rest[0])
    elif cmd == "verify":
        with _lock(packs, fcntl.LOCK_SH):
            sys.exit(verify(packs, only))
    elif cmd == "reindex":
        with _lock(packs, fcntl.LOCK_EX):
            sys.exit(reindex(packs))
    elif cmd == "push":
        push(packs, gc="--gc" in rest)
    elif cmd == "coverage":
        with _lock(packs, fcntl.LOCK_SH):
            coverage(packs, rest[0] if rest and not rest[0].startswith("--") else None)
    elif cmd == "gc-offsite":
        gc_offsite(packs)
    elif cmd == "pull":
        with _lock(packs, fcntl.LOCK_EX):
            pull(packs, only)
    else:
        sys.exit(f"unknown command {cmd}")
