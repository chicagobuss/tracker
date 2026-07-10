#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["mcp>=1.2", "httpx>=0.27"]
# ///
"""DEPRECATED: tracker serves MCP natively at POST /mcp (Streamable HTTP).
Register with:  claude mcp add --transport http tracker <base>/mcp \
                  --header "X-Actor: <name>"
This stdio script remains for one release for clients that can't speak HTTP MCP.

MCP server for tracker — the coordination store for coding agents.

Exposes tracker's REST API as MCP tools so any agent (Claude Code, etc.) can read
shared docs/folios, see who is editing, and write updates safely under its own
identity. Config via env:

    TRACKER_URL    base url (default http://127.0.0.1:8080)
    TRACKER_ACTOR  this agent's identity, stamped on every write (default claude-code)
    TRACKER_TOKEN  bearer token, only if the server has API_TOKENS set
"""
import os
import socket
from urllib.parse import urlparse
import httpx
from mcp.server.fastmcp import FastMCP

BASE = os.environ.get("TRACKER_URL", "http://127.0.0.1:8080").rstrip("/")
TOKEN = os.environ.get("TRACKER_TOKEN", "")


def _local_ip() -> str:
    """The local IP this host uses to reach tracker (e.g. its ZeroTier address)."""
    try:
        u = urlparse(BASE)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((u.hostname or "127.0.0.1", u.port or 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "?"


def _resolve_actor() -> str:
    """Actor identity = <role>@<hostname>/<ip>, so attribution names not just the
    agent role but the machine it ran on. Override the role with TRACKER_ACTOR;
    set TRACKER_ACTOR_FULL to bypass the host/ip suffix entirely."""
    if full := os.environ.get("TRACKER_ACTOR_FULL"):
        return full
    role = os.environ.get("TRACKER_ACTOR", "claude-code")
    return f"{role}@{socket.gethostname()}/{_local_ip()}"


ACTOR = _resolve_actor()

mcp = FastMCP("tracker")


def _headers(mutating: bool = False) -> dict:
    h = {}
    if TOKEN:
        h["Authorization"] = f"Bearer {TOKEN}"
    if mutating:
        h["X-Actor"] = ACTOR
    return h


def _client() -> httpx.Client:
    return httpx.Client(base_url=BASE, timeout=30, headers=_headers())


# --- reads ---

@mcp.tool()
def search_docs(query: str = "", kind: str = "", tag: str = "", limit: int = 50) -> dict:
    """Search/list documents. `query` is a full-text search (understands quoted
    "phrases", OR, and -negation; bare words must ALL match). Filter by `kind`
    (note, spec, folio) and/or an exact `tag` (e.g. folio:tracker, topic:zpq).
    Returns a compact table — `cols` names the columns, `rows` are the values
    (address a doc by its `slug`); read content with read_doc. `hint` appears
    when a query matched nothing."""
    with _client() as c:
        r = c.get("/docs", params={"q": query, "kind": kind, "tag": tag,
                                   "view": "table", "limit": limit})
        r.raise_for_status()
        d = r.json()
        out = {"count": d["count"], "total": d["total"],
               "cols": d.get("cols"), "rows": d.get("rows")}
        if d.get("hint"):
            out["hint"] = d["hint"]
        return out


@mcp.tool()
def list_folios() -> dict:
    """List all folios (collections of related documents, like gists). Returns a
    compact table (`cols`/`rows`); address a folio by its `slug`."""
    with _client() as c:
        r = c.get("/folios", params={"view": "table"})
        r.raise_for_status()
        d = r.json()
        return {"count": d["count"], "cols": d.get("cols"), "rows": d.get("rows")}


@mcp.tool()
def list_tags() -> dict:
    """List the whole tag vocabulary with usage counts, so you can discover what
    tags exist (e.g. folio:*, topic:*, kind:*) without reading every document."""
    with _client() as c:
        r = c.get("/tags")
        r.raise_for_status()
        d = r.json()
        return {"count": d["count"], "tags": d["tags"]}


@mcp.tool()
def get_folio(slug: str) -> dict:
    """Get a folio (by slug) and the list of files it contains."""
    with _client() as c:
        r = c.get(f"/folios/{slug}")
        r.raise_for_status()
        return r.json()


@mcp.tool()
def read_doc(id_or_slug: str) -> dict:
    """Read a document's metadata AND its current text content. Accepts a UUID
    or slug. Also reports if another agent currently holds the write-lease."""
    with _client() as c:
        meta = c.get(f"/docs/{id_or_slug}")
        meta.raise_for_status()
        data = meta.json()
        doc = data["document"]
        content = ""
        if doc.get("content_key"):
            raw = c.get(f"/docs/{doc['id']}/raw")
            if raw.status_code == 200:
                content = raw.text
        return {"document": doc, "content": content, "lock": data.get("lock")}


@mcp.tool()
def who_is_editing(id_or_slug: str) -> dict:
    """Check whether a document is currently being written by another agent
    (its live lease), so you don't collide. Returns {locked: bool, lease?}."""
    with _client() as c:
        r = c.get(f"/docs/{id_or_slug}/lock")
        r.raise_for_status()
        return r.json()


@mcp.tool()
def list_actors() -> dict:
    """List entities (agents/humans) that have acted on the store, with
    first_seen / last_seen / action_count — i.e. who's active."""
    with _client() as c:
        r = c.get("/actors")
        r.raise_for_status()
        actors = r.json()["actors"]
        return {"count": len(actors), "actors": actors}


# --- writes (stamped with TRACKER_ACTOR) ---

@mcp.tool()
def create_doc(slug: str, title: str = "", kind: str = "note", content: str = "",
               tags: list[str] | None = None, folio: str = "") -> dict:
    """Create a document. If `folio` (a folio slug) is given, `slug` is treated as
    the filename within that folio (the server namespaces it). `content` seeds v1."""
    with httpx.Client(base_url=BASE, timeout=30, headers=_headers(mutating=True)) as c:
        if folio:
            r = c.post(f"/folios/{folio}/files",
                       json={"filename": slug, "title": title, "kind": kind, "content": content})
        else:
            r = c.post("/docs", json={"slug": slug, "title": title or slug, "kind": kind,
                                      "tags": tags or [], "content": content})
        r.raise_for_status()
        return r.json().get("document", r.json())


@mcp.tool()
def retag(id_or_slug: str, add_tags: list[str] | None = None,
          remove_tags: list[str] | None = None, metadata: dict | None = None) -> dict:
    """Change a document's tags and/or metadata WITHOUT rewriting its content.
    No lease needed and the version is unchanged — tags/metadata are labels, not
    content. Use namespaced tags (topic:zpq, kind:agent-reply, status:draft).
    `add_tags`/`remove_tags` adjust the set; `metadata` is shallow-merged."""
    body: dict = {}
    if add_tags:
        body["add_tags"] = add_tags
    if remove_tags:
        body["remove_tags"] = remove_tags
    if metadata is not None:
        body["metadata"] = metadata
    with httpx.Client(base_url=BASE, timeout=30, headers=_headers(mutating=True)) as c:
        r = c.patch(f"/docs/{id_or_slug}", json=body)
        r.raise_for_status()
        return r.json().get("document", r.json())


@mcp.tool()
def create_folio(slug: str, description: str = "", public: bool = False) -> dict:
    """Create a folio (a collection). Its files are added with create_doc(folio=slug)."""
    with httpx.Client(base_url=BASE, timeout=30, headers=_headers(mutating=True)) as c:
        r = c.post("/folios", json={"slug": slug, "description": description, "public": public})
        r.raise_for_status()
        return r.json().get("folio", r.json())


@mcp.tool()
def update_doc(id_or_slug: str, content: str) -> dict:
    """Safely replace a document's content. Handles the coordination dance for
    you: acquires the write-lease, writes with optimistic version check, releases.
    Fails clearly (without writing) if another agent holds the lease."""
    with httpx.Client(base_url=BASE, timeout=30, headers=_headers(mutating=True)) as c:
        meta = c.get(f"/docs/{id_or_slug}")
        meta.raise_for_status()
        doc = meta.json()["document"]
        did, version = doc["id"], doc["version"]

        lock = c.post(f"/docs/{did}/lock", json={"reason": f"{ACTOR} updating content"})
        if lock.status_code == 409:
            return {"ok": False, "error": "locked by another agent", "detail": lock.json()}
        lock.raise_for_status()
        token = lock.json()["lock"]["lease_token"]
        try:
            put = c.put(f"/docs/{did}", content=content.encode(),
                        headers={"X-Lease-Token": token, "If-Match": str(version),
                                 "Content-Type": doc.get("content_type", "text/markdown")})
            if put.status_code == 412:
                return {"ok": False, "error": "version changed under you; re-read and retry"}
            put.raise_for_status()
            return {"ok": True, "document": put.json()["document"]}
        finally:
            c.request("DELETE", f"/docs/{did}/lock", headers={"X-Lease-Token": token})


# --- tasks ---

@mcp.tool()
def create_task(title: str, payload: dict | None = None) -> dict:
    """Enqueue a task for agents to claim."""
    with httpx.Client(base_url=BASE, timeout=30, headers=_headers(mutating=True)) as c:
        r = c.post("/tasks", json={"title": title, "payload": payload or {}})
        r.raise_for_status()
        return r.json().get("task", r.json())


@mcp.tool()
def claim_task() -> dict:
    """Atomically claim the next open task (no two agents get the same one).
    Returns the task, or {claimed: false} if the queue is empty."""
    with httpx.Client(base_url=BASE, timeout=30, headers=_headers(mutating=True)) as c:
        r = c.post("/tasks/claim", json={})
        r.raise_for_status()
        task = r.json().get("task")
        return {"claimed": bool(task), "task": task}


@mcp.tool()
def complete_task(task_id: str, status: str = "done", result: dict | None = None) -> dict:
    """Mark a task done|failed with an optional result."""
    with httpx.Client(base_url=BASE, timeout=30, headers=_headers(mutating=True)) as c:
        r = c.post(f"/tasks/{task_id}/complete", json={"status": status, "result": result or {}})
        r.raise_for_status()
        return r.json().get("task", r.json())


if __name__ == "__main__":
    mcp.run()
