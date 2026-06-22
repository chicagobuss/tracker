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
import os, sys
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


CMDS = {
    "download-blobs": lambda a: download_blobs(a[0]),
    "upload-blobs": lambda a: upload_blobs(a[0]),
    "put-archive": lambda a: put_archive(a[0], a[1] if len(a) > 1 else None),
    "get-archive": lambda a: get_archive(a[0], a[1]),
    "list-archives": lambda a: list_archives(),
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in CMDS:
        sys.exit(f"usage: s3util.py [{' | '.join(CMDS)}] ...")
    CMDS[sys.argv[1]](sys.argv[2:])
