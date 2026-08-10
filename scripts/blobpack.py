#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["boto3"]
# ///
"""Blob packs: the backup's copy of the content store, as a few files.

Blobs are immutable and named by their own sha256, so they are appended to pack
files rather than stored one-per-file. A pack is sealed once it passes
BACKUP_PACK_MAX_BYTES; a sealed pack never changes again, so it is uploaded
exactly once and read only during a restore. Only the open pack is ever
rewritten.

Storing them individually costs more in per-file block overhead than the data
occupies, and forces a directory that grows without bound — which in turn forces
sharding, incremental cursors, per-key presence checks and a garbage-collection
keep-set. Packing removes all of that: "what do I already have" is one index
file, not a walk of a directory with millions of entries.

  blobpack.py fold   <dir>              copy new content-store blobs into the open pack
  blobpack.py emit   <dir> <keyfile>    write those keys to the content store (restore)
  blobpack.py verify <dir> [--packs a,b]  re-hash every blob in the packs
  blobpack.py push   <dir>              mirror packs offsite (sealed ones once)
  blobpack.py pull   <dir> [--packs a,b]  fetch packs from offsite
"""
import hashlib
import io
import json
import os
import sys
import tarfile
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

PACK_MAX = int(os.environ.get("BACKUP_PACK_MAX_BYTES", 64 * 1024 * 1024))
PAR = int(os.environ.get("BACKUP_PARALLEL", "16"))


def _client(prefix):
    ep = os.environ[f"{prefix}S3_ENDPOINT"]
    if not ep.startswith("http"):
        ep = ("https://" if os.environ.get("S3_USE_SSL") == "true" else "http://") + ep
    return boto3.client("s3", endpoint_url=ep,
                        aws_access_key_id=os.environ[f"{prefix}S3_ACCESS_KEY"],
                        aws_secret_access_key=os.environ[f"{prefix}S3_SECRET_KEY"],
                        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"))


def content(): return _client(""), os.environ["S3_BUCKET"]
def backup_store():
    if not os.environ.get("BACKUP_S3_ENDPOINT"):
        sys.exit("BACKUP_S3_* must be set for offsite pack operations")
    return _client("BACKUP_"), os.environ["BACKUP_S3_BUCKET"]


def packs_dir(work): return os.path.join(work, "packs")
def pack_path(work, name): return os.path.join(packs_dir(work), name)
def offsite_prefix():
    base = os.environ.get("BACKUP_S3_PREFIX", "").strip("/")
    return (base + "/packs/").lstrip("/")


# --- index -----------------------------------------------------------------
# key -> pack name. Checksummed: it is now the only record of where a blob
# lives, so silent truncation would strand blobs that are physically present.

def read_index(work):
    p = pack_path(work, "INDEX")
    if not os.path.exists(p):
        return {}
    raw = open(p, "rb").read()
    sums = p + ".sha256"
    if os.path.exists(sums):
        want = open(sums).read().split()[0]
        got = hashlib.sha256(raw).hexdigest()
        if want != got:
            sys.exit(f"INDEX checksum mismatch (want {want[:12]}…, got {got[:12]}…) — refusing to use it")
    idx = {}
    for line in raw.decode().splitlines():
        k, _, name = line.strip().partition(" ")
        if k:
            idx[k] = name
    return idx


def write_index(work, idx):
    p = pack_path(work, "INDEX")
    body = "".join(f"{k} {idx[k]}\n" for k in sorted(idx)).encode()
    with open(p + ".tmp", "wb") as f:
        f.write(body)
    os.replace(p + ".tmp", p)
    with open(p + ".sha256.tmp", "w") as f:
        f.write(hashlib.sha256(body).hexdigest() + "  INDEX\n")
    os.replace(p + ".sha256.tmp", p + ".sha256")


def _today():
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def open_pack(work, idx):
    """The pack currently accepting writes.

    Sealed by size OR by day, whichever comes first. Size alone is not enough:
    the open pack is re-sent offsite on every run, so a pack that takes months
    to reach the size cap means months of re-uploading the same tens of
    megabytes hourly. Rolling daily bounds that to one day's blobs, and the
    size cap still bounds a single pack during a burst.
    """
    today = _today()
    same_day = sorted(n for n in set(idx.values()) if n.startswith(f"blobs-{today}-"))
    if same_day:
        last = same_day[-1]
        if os.path.getsize(pack_path(work, last)) < PACK_MAX:
            return last
        seq = int(last.rsplit("-", 1)[1].split(".")[0]) + 1
        return f"blobs-{today}-{seq:03d}.tar"
    return f"blobs-{today}-001.tar"


def _check(key, data):
    if key.startswith("sha256/") and hashlib.sha256(data).hexdigest() != key.split("/", 1)[1]:
        return False
    return True


# --- commands --------------------------------------------------------------

def fold(work, keys):
    """Copy content-store blobs the packs do not hold yet into the open pack."""
    os.makedirs(packs_dir(work), exist_ok=True)
    idx = read_index(work)
    missing = sorted(set(keys) - idx.keys())
    if not missing:
        print(f"packs: 0 new, {len(idx)} held across {len(set(idx.values()))} pack(s)")
        return
    s3, bucket = content()
    with ThreadPoolExecutor(max_workers=PAR) as ex:
        blobs = list(ex.map(lambda k: (k, s3.get_object(Bucket=bucket, Key=k)["Body"].read()), missing))
    # Keep one pack open at a time and roll over when it passes the seal size,
    # rather than reopening the tar per blob — the initial fold is thousands of
    # blobs and that difference is the whole runtime.
    added = 0
    name = open_pack(work, idx)
    p = pack_path(work, name)
    tf = tarfile.open(p, "a" if os.path.exists(p) else "w")
    try:
        for k, data in blobs:
            if not _check(k, data):
                sys.exit(f"content store blob {k} does not match its own hash — refusing to pack it")
            if tf.fileobj.tell() >= PACK_MAX:
                tf.close()
                seq = int(name.rsplit("-", 1)[1].split(".")[0]) + 1
                name = f"blobs-{_today()}-{seq:03d}.tar"
                p = pack_path(work, name)
                tf = tarfile.open(p, "w")
            ti = tarfile.TarInfo(k)
            ti.size = len(data)
            tf.addfile(ti, io.BytesIO(data))
            idx[k] = name
            added += 1
    finally:
        tf.close()
    write_index(work, idx)
    sealed = sum(1 for n in set(idx.values()) if not n.startswith(f"blobs-{_today()}-"))
    print(f"packs: {added} new into {open_pack(work, idx)}, {len(idx)} held across "
          f"{len(set(idx.values()))} pack(s) ({sealed} sealed)")


def verify(work, only=None):
    idx = read_index(work)
    names = sorted(only or {v for v in idx.values()})
    bad = n = 0
    for name in names:
        p = pack_path(work, name)
        if not os.path.exists(p):
            print(f"  MISSING pack {name}")
            bad += 1
            continue
        with tarfile.open(p) as tf:
            for m in tf:
                data = tf.extractfile(m).read()
                n += 1
                if not _check(m.name, data):
                    print(f"  CORRUPT {m.name} in {name}")
                    bad += 1
    print(f"verify: {n} blobs across {len(names)} pack(s), {bad} problem(s)")
    return 1 if bad else 0


def emit(work, keyfile):
    """Restore path: write exactly these keys to the content store."""
    keys = {k.strip() for k in open(keyfile) if k.strip()}
    idx = read_index(work)
    absent = sorted(keys - idx.keys())
    if absent:
        sys.exit(f"packs cannot satisfy this restore: {len(absent)} key(s) not in any pack "
                 f"(e.g. {absent[0]}) — refusing a partial restore")
    by_pack = {}
    for k in keys:
        by_pack.setdefault(idx[k], set()).add(k)
    missing = [n for n in by_pack if not os.path.exists(pack_path(work, n))]
    if missing:
        sys.exit(f"cannot restore: pack file(s) absent: {', '.join(sorted(missing))}")
    s3, bucket = content()
    if not any(b["Name"] == bucket for b in s3.list_buckets().get("Buckets", [])):
        s3.create_bucket(Bucket=bucket)
    sent = 0
    for name, want in sorted(by_pack.items()):
        with tarfile.open(pack_path(work, name)) as tf:
            batch = []
            for m in tf:
                if m.name in want:
                    data = tf.extractfile(m).read()
                    if not _check(m.name, data):
                        sys.exit(f"pack {name} holds a corrupt blob for {m.name} — refusing to restore corruption")
                    batch.append((m.name, data))
        with ThreadPoolExecutor(max_workers=PAR) as ex:
            list(ex.map(lambda t: s3.put_object(Bucket=bucket, Key=t[0], Body=t[1]), batch))
        sent += len(batch)
    print(f"restored {sent} blobs from {len(by_pack)} pack(s) to {bucket}")


def push(work):
    """Mirror packs offsite. Sealed packs are uploaded once and never again."""
    s3, bucket = backup_store()
    if not any(b["Name"] == bucket for b in s3.list_buckets().get("Buckets", [])):
        s3.create_bucket(Bucket=bucket)
    pre = offsite_prefix()
    have = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=pre):
        for o in page.get("Contents", []):
            have[o["Key"][len(pre):]] = o["Size"]
    idx = read_index(work)
    sent = skipped = 0
    for name in sorted(set(idx.values())):
        p = pack_path(work, name)
        size = os.path.getsize(p)
        # A sealed pack of the same size is already up there, byte for byte:
        # its contents can never change again. Only today's packs are re-sent.
        if have.get(name) == size and not name.startswith(f"blobs-{_today()}-"):
            skipped += 1
            continue
        s3.upload_file(p, bucket, pre + name)
        sent += 1
    for extra in ("INDEX", "INDEX.sha256"):
        s3.upload_file(pack_path(work, extra), bucket, pre + extra)
    print(f"pack push: {sent} uploaded, {skipped} sealed pack(s) already offsite")


def pull(work, only=None):
    s3, bucket = backup_store()
    pre = offsite_prefix()
    os.makedirs(packs_dir(work), exist_ok=True)
    todo = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=pre):
        for o in page.get("Contents", []):
            rel = o["Key"][len(pre):]
            if only and rel not in set(only) | {"INDEX", "INDEX.sha256"}:
                continue
            dst = pack_path(work, rel)
            if os.path.exists(dst) and os.path.getsize(dst) == o["Size"]:
                continue
            todo.append((o["Key"], dst))
    with ThreadPoolExecutor(max_workers=PAR) as ex:
        list(ex.map(lambda t: s3.download_file(bucket, t[0], t[1]), todo))
    print(f"pack pull: {len(todo)} file(s) fetched into {packs_dir(work)}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    cmd, work = sys.argv[1], sys.argv[2]
    rest = sys.argv[3:]
    packs = None
    if "--packs" in rest:
        packs = rest[rest.index("--packs") + 1].split(",")
    if cmd == "fold":
        fold(work, [l.strip() for l in sys.stdin if l.strip()])
    elif cmd == "emit":
        emit(work, rest[0])
    elif cmd == "verify":
        sys.exit(verify(work, packs))
    elif cmd == "push":
        push(work)
    elif cmd == "pull":
        pull(work, packs)
    else:
        sys.exit(f"unknown command {cmd}")
