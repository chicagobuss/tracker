#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["boto3"]
# ///
"""S3 helper for tracker backup/restore. Two buckets:

  - the content store (S3_* env): tracker's blobs   -> download-blobs / upload-blobs
  - a backup destination (BACKUP_S3_* env): where tarballs live (R2 or S3)
      -> put-archive / get-archive / list-archives

All endpoints are S3-compatible (RustFS, AWS S3, Cloudflare R2).
"""
import hashlib
import os, sys
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.config import Config


def client(endpoint, key, secret):
    if not endpoint.startswith("http"):
        endpoint = ("https://" if os.environ.get("S3_USE_SSL") == "true" else "http://") + endpoint
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=key, aws_secret_access_key=secret,
                        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"))


def content_client():
    return client(os.environ["S3_ENDPOINT"], os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"])


def backup_client():
    ep = os.environ.get("BACKUP_S3_ENDPOINT")
    if not ep:
        sys.exit("BACKUP_S3_ENDPOINT/BUCKET/ACCESS_KEY/SECRET_KEY must be set for archive ops")
    return client(ep, os.environ["BACKUP_S3_ACCESS_KEY"], os.environ["BACKUP_S3_SECRET_KEY"])


def ensure_bucket(s3, bucket):
    if not any(b["Name"] == bucket for b in s3.list_buckets().get("Buckets", [])):
        s3.create_bucket(Bucket=bucket)


def download_blobs(dst):
    s3, b = content_client(), os.environ["S3_BUCKET"]
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b):
        for o in page.get("Contents", []):
            p = os.path.join(dst, o["Key"])
            os.makedirs(os.path.dirname(p), exist_ok=True)
            s3.download_file(b, o["Key"], p)
            n += 1
    print(f"downloaded {n} blobs from {b}")


def upload_blobs(src):
    s3, b = content_client(), os.environ["S3_BUCKET"]
    ensure_bucket(s3, b)
    n = 0
    for root, _, files in os.walk(src):
        for f in files:
            full = os.path.join(root, f)
            key = os.path.relpath(full, src)
            s3.upload_file(full, b, key)
            n += 1
    print(f"uploaded {n} blobs to {b}")


def put_archive(path, name=None):
    s3, b = backup_client(), os.environ["BACKUP_S3_BUCKET"]
    ensure_bucket(s3, b)
    key = (os.environ.get("BACKUP_S3_PREFIX", "").strip("/") + "/" + (name or os.path.basename(path))).lstrip("/")
    s3.upload_file(path, b, key)
    print(f"uploaded archive -> s3://{b}/{key}")


def get_archive(name, dest):
    s3, b = backup_client(), os.environ["BACKUP_S3_BUCKET"]
    key = (os.environ.get("BACKUP_S3_PREFIX", "").strip("/") + "/" + name).lstrip("/")
    s3.download_file(b, key, dest)
    print(f"downloaded s3://{b}/{key} -> {dest}")


def list_archives():
    s3, b = backup_client(), os.environ["BACKUP_S3_BUCKET"]
    prefix = os.environ.get("BACKUP_S3_PREFIX", "").strip("/")
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=prefix):
        for o in page.get("Contents", []):
            print(f"  {o['Key']}  {o['Size']} bytes  {o['LastModified']:%Y-%m-%d %H:%M}")



# --- blob pool -------------------------------------------------------------
# The pool is an append-only, content-addressed mirror of the content store,
# shared by every snapshot. Blobs are immutable and named by their own sha256,
# so a key already in the pool can never need re-fetching — that is what makes
# a backup run cost O(new blobs) instead of O(all blobs).

def _pool_path(pool, key):
    return os.path.join(pool, key)


def sync_pool(pool):
    """Copy content-store objects the pool does not already hold."""
    s3, b = content_client(), os.environ["S3_BUCKET"]
    have = set()
    for root, _, files in os.walk(pool):
        for f in files:
            have.add(os.path.relpath(os.path.join(root, f), pool))
    seen = 0
    todo = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b):
        for o in page.get("Contents", []):
            seen += 1
            if o["Key"] not in have:
                todo.append(o["Key"])
    for k in todo:
        os.makedirs(os.path.dirname(_pool_path(pool, k)), exist_ok=True)
    if todo:
        with ThreadPoolExecutor(max_workers=int(os.environ.get("BACKUP_PARALLEL", "16"))) as ex:
            list(ex.map(lambda k: s3.download_file(b, k, _pool_path(pool, k)), todo))
    print(f"pool sync: {seen} in store, {len(todo)} newly fetched, {seen - len(todo)} already held")


def verify_pool(pool, keys_file, deep=False):
    """Every key a snapshot needs must be present; optionally hash-check bytes.

    Presence is a stat per key and stays cheap at any scale, so it runs on every
    backup. Hashing reads every byte in the pool, which is O(total bytes) — the
    very shape this design exists to avoid — so it is opt-in (`--deep`) for a
    periodic integrity sweep, and always on during a restore, where correctness
    matters more than speed and the cost is paid once.
    """
    keys = [k.strip() for k in open(keys_file) if k.strip()]
    missing, corrupt = [], []
    for k in keys:
        p = _pool_path(pool, k)
        if not os.path.exists(p):
            missing.append(k)
            continue
        if deep and k.startswith("sha256/"):
            h = hashlib.sha256(open(p, "rb").read()).hexdigest()
            if h != k.split("/", 1)[1]:
                corrupt.append(k)
    if missing or corrupt:
        for k in missing[:10]:
            print(f"  MISSING {k}")
        for k in corrupt[:10]:
            print(f"  CORRUPT {k}")
        sys.exit(f"pool verify FAILED: {len(missing)} missing, {len(corrupt)} corrupt of {len(keys)}")
    print(f"pool verify OK: {len(keys)} keys present" + (" and hash-correct" if deep else ""))


def upload_from_pool(pool, keys_file):
    """Restore path: push exactly the keys a snapshot references.

    Hash-checks each blob on the way out. A restore is where a silently corrupt
    pool would become permanent, so it is the one place worth the full read.
    """
    s3, b = content_client(), os.environ["S3_BUCKET"]
    ensure_bucket(s3, b)
    keys = [k.strip() for k in open(keys_file) if k.strip()]
    for k in keys:
        p = _pool_path(pool, k)
        if not os.path.exists(p):
            sys.exit(f"pool is missing {k} — refusing a partial restore")
        if k.startswith("sha256/"):
            h = hashlib.sha256(open(p, "rb").read()).hexdigest()
            if h != k.split("/", 1)[1]:
                sys.exit(f"pool blob {k} does not match its own hash — refusing to restore corruption")
        s3.upload_file(p, b, k)
    print(f"uploaded {len(keys)} blobs from pool to {b}")


def _pool_prefix():
    base = os.environ.get("BACKUP_S3_PREFIX", "").strip("/")
    return (base + "/pool/").lstrip("/")


def push_pool(pool):
    """Mirror the local pool into the backup store, uploading only what is new.

    Without this the offsite copy is a snapshot that references bytes living
    only on the machine being backed up — which is not an offsite backup. Blobs
    are immutable, so presence of the key is sufficient: never re-upload.
    """
    s3, b = backup_client(), os.environ["BACKUP_S3_BUCKET"]
    ensure_bucket(s3, b)
    pre = _pool_prefix()
    have = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=pre):
        for o in page.get("Contents", []):
            have.add(o["Key"][len(pre):])
    todo, held = [], 0
    for root, _, files in os.walk(pool):
        for f in files:
            full = os.path.join(root, f)
            key = os.path.relpath(full, pool)
            if key in have:
                held += 1
            else:
                todo.append((full, key))
    # Thousands of small objects over a WAN are latency-bound, not
    # bandwidth-bound, so upload them concurrently. Steady state is a handful of
    # blobs; this matters for the initial seed and for a rebuild.
    if todo:
        with ThreadPoolExecutor(max_workers=int(os.environ.get("BACKUP_PARALLEL", "16"))) as ex:
            list(ex.map(lambda t: s3.upload_file(t[0], b, pre + t[1]), todo))
    print(f"pool push: {len(todo)} uploaded, {held} already offsite")


def pull_pool(pool):
    """Disaster recovery: rebuild a local pool from the backup store."""
    s3, b = backup_client(), os.environ["BACKUP_S3_BUCKET"]
    pre = _pool_prefix()
    todo = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=pre):
        for o in page.get("Contents", []):
            rel = o["Key"][len(pre):]
            dst = os.path.join(pool, rel)
            if not os.path.exists(dst):
                todo.append((o["Key"], dst))
    for _, dst in todo:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
    if todo:
        with ThreadPoolExecutor(max_workers=int(os.environ.get("BACKUP_PARALLEL", "16"))) as ex:
            list(ex.map(lambda t: s3.download_file(b, t[0], t[1]), todo))
    print(f"pool pull: {len(todo)} blobs fetched into {pool}")


def gc_pool(pool, keep_file):
    """Delete pool objects no retained snapshot and no live doc references."""
    keep = {k.strip() for k in open(keep_file) if k.strip()}
    removed = kept = 0
    for root, _, files in os.walk(pool):
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), pool)
            if rel in keep:
                kept += 1
            else:
                os.remove(os.path.join(root, f))
                removed += 1
    print(f"pool gc: kept {kept}, removed {removed}")


CMDS = {
    "download-blobs": lambda a: download_blobs(a[0]),
    "upload-blobs": lambda a: upload_blobs(a[0]),
    "put-archive": lambda a: put_archive(a[0], a[1] if len(a) > 1 else None),
    "get-archive": lambda a: get_archive(a[0], a[1]),
    "list-archives": lambda a: list_archives(),
    "sync-pool": lambda a: sync_pool(a[0]),
    "verify-pool": lambda a: verify_pool(a[0], a[1], "--deep" in a),
    "upload-from-pool": lambda a: upload_from_pool(a[0], a[1]),
    "gc-pool": lambda a: gc_pool(a[0], a[1]),
    "push-pool": lambda a: push_pool(a[0]),
    "pull-pool": lambda a: pull_pool(a[0]),
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in CMDS:
        sys.exit(f"usage: s3util.py [{' | '.join(CMDS)}] ...")
    CMDS[sys.argv[1]](sys.argv[2:])
