#!/usr/bin/env python3
"""Import recent GitHub gists into tracker as folios.

Each gist -> a folio document (kind=folio, metadata carries the gist's
description/public/id). Each gist file -> a document tagged folio:<slug> with its
content seeded. Idempotent: a folio whose slug already exists is skipped.

GitHub data is read via the `gh` CLI (uses your existing auth). Usage:

    python3 import_gists.py [--since-days 365] [--tracker http://127.0.0.1:8770] \
                            [--actor importer] [--dry-run]
"""
import argparse, datetime, json, re, subprocess, sys
import urllib.request, urllib.parse, urllib.error

def gh_json(path, paginate=False):
    cmd = ["gh", "api"] + (["--paginate"] if paginate else []) + [path]
    return json.loads(subprocess.check_output(cmd))

def gh_token():
    return subprocess.check_output(["gh", "auth", "token"]).decode().strip()

def slugify(s):
    s = re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")
    return s[:60]

def post(tracker, actor, body, dry):
    if dry:
        print(f"    [dry-run] POST /docs slug={body['slug']}")
        return 201
    data = json.dumps(body).encode()
    req = urllib.request.Request(tracker + "/docs", data=data, method="POST",
        headers={"Content-Type": "application/json", "X-Actor": actor})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status
    except urllib.error.HTTPError as e:
        print(f"    ! {e.code} {e.read().decode()[:120]} (slug={body['slug']})")
        return e.code

def exists(tracker, slug):
    try:
        urllib.request.urlopen(tracker + "/docs/" + urllib.parse.quote(slug, safe=""))
        return True
    except urllib.error.HTTPError as e:
        return e.code != 404

def fetch_raw(url, token):
    req = urllib.request.Request(url, headers={"Authorization": "token " + token})
    with urllib.request.urlopen(req) as r:
        return r.read().decode("utf-8", "replace")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-days", type=int, default=365)
    ap.add_argument("--tracker", default="http://127.0.0.1:8770")
    ap.add_argument("--actor", default="importer")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=args.since_days)
    token = gh_token()

    gists = gh_json("/gists", paginate=True)
    recent = [g for g in gists if datetime.datetime.fromisoformat(g["created_at"]) >= cutoff]
    print(f"{len(gists)} gists total; {len(recent)} created in the last {args.since_days} days\n")

    folios = files = skipped = 0
    for g in recent:
        gid = g["id"]
        desc = (g.get("description") or "").strip()
        base = slugify(desc) or "gist"
        slug = f"{base}-{gid[:7]}"
        if exists(args.tracker, slug):
            print(f"= skip {slug} (already imported)")
            skipped += 1
            continue
        print(f"+ folio {slug}  ({'public' if g['public'] else 'secret'}, {len(g['files'])} files)  {desc[:50]}")
        meta = {"github_id": gid, "public": g["public"], "description": desc,
                "source_created_at": g["created_at"], "source": "github-gist"}
        post(args.tracker, args.actor,
             {"slug": slug, "title": desc or slug, "kind": "folio", "metadata": meta}, args.dry_run)
        folios += 1

        full = gh_json(f"/gists/{gid}")
        for fname, f in full["files"].items():
            content = f.get("content") or ""
            if f.get("truncated"):
                content = fetch_raw(f["raw_url"], token)
            ctype = "text/markdown" if fname.lower().endswith((".md", ".markdown")) else "text/plain"
            body = {
                "slug": f"{slug}/{fname}", "title": fname, "kind": "note",
                "tags": [f"folio:{slug}"], "content": content, "content_type": ctype,
                "metadata": {"filename": fname, "language": f.get("language"), "folio": slug},
            }
            if post(args.tracker, args.actor, body, args.dry_run) in (200, 201):
                files += 1

    print(f"\nDone: {folios} folios, {files} files imported, {skipped} skipped.")

if __name__ == "__main__":
    main()
