package main

// Native MCP endpoint (Streamable HTTP transport): POST /mcp accepts JSON-RPC
// and exposes the store as MCP tools, so any agent connects with just
//
//	claude mcp add --transport http tracker http://<host>:8080/mcp \
//	  --header "X-Actor: <name>" [--header "Authorization: Bearer <token>"]
//
// — no local client script. Tools-only and stateless: no session ids, no
// server-push stream (GET /mcp is 405), every response is application/json.
// Tools call the store directly rather than round-tripping through REST.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

func rpcResult(w http.ResponseWriter, id json.RawMessage, result any) {
	writeJSON(w, http.StatusOK, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func rpcError(w http.ResponseWriter, id json.RawMessage, code int, msg string) {
	writeJSON(w, http.StatusOK, map[string]any{"jsonrpc": "2.0", "id": id,
		"error": map[string]any{"code": code, "message": msg}})
}

func (s *Server) mcpHandler(w http.ResponseWriter, r *http.Request) {
	var req rpcRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&req); err != nil {
		rpcError(w, nil, -32700, "parse error: "+err.Error())
		return
	}
	// Notifications (no id) need no response body.
	if len(req.ID) == 0 || string(req.ID) == "null" {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	switch req.Method {
	case "initialize":
		// Tools-only servers work identically across protocol revisions, so
		// echo the client's version rather than forcing a downgrade dance.
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &p)
		if p.ProtocolVersion == "" {
			p.ProtocolVersion = "2025-06-18"
		}
		rpcResult(w, req.ID, map[string]any{
			"protocolVersion": p.ProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "tracker", "version": appVersion()},
		})
	case "ping":
		rpcResult(w, req.ID, map[string]any{})
	case "tools/list":
		rpcResult(w, req.ID, map[string]any{"tools": mcpToolDescriptors()})
	case "tools/call":
		var p struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			rpcError(w, req.ID, -32602, "invalid params: "+err.Error())
			return
		}
		tool, ok := mcpTools[p.Name]
		if !ok {
			rpcError(w, req.ID, -32602, "unknown tool: "+p.Name)
			return
		}
		actor := strings.TrimSpace(r.Header.Get("X-Actor"))
		if tool.mutating && actor == "" {
			rpcResult(w, req.ID, toolError(`X-Actor header required for writes — add --header "X-Actor: <your-agent-name>" to your MCP config`))
			return
		}
		out, err := tool.fn(r.Context(), s, actor, p.Arguments)
		if err != nil {
			rpcResult(w, req.ID, toolError(err.Error()))
			return
		}
		b, err := json.Marshal(out)
		if err != nil {
			rpcResult(w, req.ID, toolError("marshal result: "+err.Error()))
			return
		}
		rpcResult(w, req.ID, map[string]any{"content": []map[string]any{{"type": "text", "text": string(b)}}})
	default:
		rpcError(w, req.ID, -32601, "method not found: "+req.Method)
	}
}

func toolError(msg string) map[string]any {
	return map[string]any{"isError": true, "content": []map[string]any{{"type": "text", "text": msg}}}
}

// --- tool arguments (loosely-typed JSON) ---

type targs map[string]any

func (a targs) str(k string) string {
	v, _ := a[k].(string)
	return v
}

func (a targs) num(k string, def int) int {
	if v, ok := a[k].(float64); ok {
		return int(v)
	}
	return def
}

func (a targs) boolean(k string) bool {
	v, _ := a[k].(bool)
	return v
}

func (a targs) strs(k string) []string {
	raw, ok := a[k].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// raw returns the value marshalled back to JSON (for jsonb metadata/payload),
// or nil when absent.
func (a targs) raw(k string) json.RawMessage {
	v, ok := a[k]
	if !ok || v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}

// --- tool table ---

type mcpTool struct {
	desc     string
	schema   map[string]any
	mutating bool
	fn       func(ctx context.Context, s *Server, actor string, a targs) (any, error)
}

func obj(required []string, props map[string]any) map[string]any {
	sch := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		sch["required"] = required
	}
	return sch
}

var pStr = map[string]any{"type": "string"}
var pInt = map[string]any{"type": "integer"}
var pBool = map[string]any{"type": "boolean"}
var pObj = map[string]any{"type": "object"}
var pStrs = map[string]any{"type": "array", "items": map[string]any{"type": "string"}}
var pView = map[string]any{"type": "string", "enum": []string{"summary", "table", "full"}}

// mcpToolOrder keeps tools/list stable; mcpTools holds the implementations.
var mcpToolOrder = []string{
	"list_docs", "get_doc", "get_raw", "create_doc", "update_doc", "lock_status",
	"retag_doc", "list_tags", "list_folios", "create_folio", "get_folio",
	"get_folio_file", "add_folio_file", "list_tasks", "get_task", "enqueue_task",
	"claim_task", "complete_task", "list_actors", "actor_activity",
}

func mcpToolDescriptors() []map[string]any {
	out := make([]map[string]any, 0, len(mcpToolOrder))
	for _, name := range mcpToolOrder {
		t := mcpTools[name]
		out = append(out, map[string]any{"name": name, "description": t.desc, "inputSchema": t.schema})
	}
	return out
}

// docList renders a doc list in the requested view with count/total attached.
func docList(view, key string, docs []Document, total, limit, offset int) map[string]any {
	var resp map[string]any
	switch view {
	case "full":
		resp = map[string]any{key: docs}
	case "summary":
		resp = map[string]any{key: summaryView(docs)}
	default: // table: the token-cheapest projection, right default for agents
		resp = tableView(docs)
	}
	resp["count"], resp["total"], resp["limit"], resp["offset"] = len(docs), total, limit, offset
	return resp
}

func (s *Server) docWithExtras(ctx context.Context, doc *Document, includeContent bool) (map[string]any, error) {
	resp := map[string]any{"document": doc, "content_url": nil, "lock": nil}
	if doc.ContentKey != "" {
		if u, err := s.store.PresignGet(ctx, doc.ContentKey, 15*time.Minute); err == nil {
			resp["content_url"] = u
		}
	}
	if l, live, err := s.store.GetLease(ctx, doc.ID); err == nil && live {
		resp["lock"] = map[string]any{"owner": l.Owner, "expires_at": l.ExpiresAt}
	}
	if includeContent && doc.ContentKey != "" {
		content, err := s.readContent(ctx, doc.ContentKey)
		if err != nil {
			return nil, err
		}
		resp["content"] = content
	}
	return resp, nil
}

func (s *Server) readContent(ctx context.Context, key string) (string, error) {
	rc, err := s.store.GetContent(ctx, key)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	b, err := io.ReadAll(io.LimitReader(rc, 8<<20))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

var mcpTools = map[string]mcpTool{
	"list_docs": {
		desc: `Search/list documents. q is full-text (quoted "phrases", OR, -negation; bare words must ALL match). Filter by kind and/or exact tag (e.g. folio:tracker). Default view=table returns {cols,rows}; address a doc by its slug.`,
		schema: obj(nil, map[string]any{"q": pStr, "kind": pStr, "tag": pStr,
			"mode": map[string]any{"type": "string", "enum": []string{"web", "plain"}},
			"view": pView, "limit": pInt, "offset": pInt}),
		fn: func(ctx context.Context, s *Server, _ string, a targs) (any, error) {
			limit, offset := a.num("limit", 50), a.num("offset", 0)
			docs, total, err := s.store.ListDocuments(ctx, a.str("q"), a.str("kind"), a.str("tag"), a.str("mode"), limit, offset)
			if err != nil {
				return nil, err
			}
			resp := docList(a.str("view"), "documents", docs, total, limit, offset)
			if total == 0 && len(strings.Fields(a.str("q"))) > 1 && a.str("mode") != "plain" {
				resp["hint"] = `0 results — multi-word queries match ALL terms. Try fewer words, "a quoted phrase", or "a OR b".`
			}
			return resp, nil
		},
	},
	"get_doc": {
		desc:   "Get a document by UUID or slug; optionally include its text content. Also reports the live write-lease, if any.",
		schema: obj([]string{"id"}, map[string]any{"id": pStr, "include_content": pBool}),
		fn: func(ctx context.Context, s *Server, _ string, a targs) (any, error) {
			doc, err := s.store.GetDocument(ctx, a.str("id"))
			if err != nil {
				return nil, err
			}
			return s.docWithExtras(ctx, doc, a.boolean("include_content"))
		},
	},
	"get_raw": {
		desc:   "Get a document's raw text content by UUID or slug.",
		schema: obj([]string{"id"}, map[string]any{"id": pStr}),
		fn: func(ctx context.Context, s *Server, _ string, a targs) (any, error) {
			doc, err := s.store.GetDocument(ctx, a.str("id"))
			if err != nil {
				return nil, err
			}
			if doc.ContentKey == "" {
				return nil, errors.New("document has no content yet")
			}
			content, err := s.readContent(ctx, doc.ContentKey)
			if err != nil {
				return nil, err
			}
			return map[string]any{"content": content, "content_type": doc.ContentType, "version": doc.Version}, nil
		},
	},
	"create_doc": {
		desc:     "Create a document. content (optional) seeds version 1.",
		mutating: true,
		schema: obj([]string{"slug"}, map[string]any{"slug": pStr, "title": pStr, "kind": pStr,
			"tags": pStrs, "metadata": pObj, "content": pStr, "content_type": pStr}),
		fn: func(ctx context.Context, s *Server, actor string, a targs) (any, error) {
			title := a.str("title")
			if title == "" {
				title = a.str("slug")
			}
			var content []byte
			if c := a.str("content"); c != "" {
				content = []byte(c)
			}
			doc, err := s.store.CreateDocument(ctx, a.str("slug"), title, a.str("kind"), a.strs("tags"),
				a.raw("metadata"), content, a.str("content_type"), actor)
			if err != nil {
				return nil, err
			}
			return map[string]any{"document": doc}, nil
		},
	},
	"update_doc": {
		desc:     "Safely replace a document's content: acquires the write-lease, writes with the version check, releases. Fails without writing if another agent holds the lease or the version moved.",
		mutating: true,
		schema: obj([]string{"id", "content"}, map[string]any{"id": pStr, "content": pStr,
			"content_type": pStr, "ttl_seconds": pInt, "reason": pStr}),
		fn: func(ctx context.Context, s *Server, actor string, a targs) (any, error) {
			doc, err := s.store.GetDocument(ctx, a.str("id"))
			if err != nil {
				return nil, err
			}
			reason := a.str("reason")
			if reason == "" {
				reason = actor + " updating content"
			}
			ttl := time.Duration(a.num("ttl_seconds", 60)) * time.Second
			lease, err := s.store.AcquireLease(ctx, doc.ID, actor, reason, ttl, "")
			if errors.Is(err, ErrLeaseHeld) {
				return nil, fmt.Errorf("locked by %s until %s — retry later", lease.Owner, lease.ExpiresAt.Format(time.RFC3339))
			}
			if err != nil {
				return nil, err
			}
			defer func() { _ = s.store.ReleaseLease(context.WithoutCancel(ctx), doc.ID, lease.LeaseToken) }()
			ctype := a.str("content_type")
			if ctype == "" {
				ctype = doc.ContentType
			}
			updated, err := s.store.WriteContent(ctx, doc.ID, actor, lease.LeaseToken, doc.Version, ctype, []byte(a.str("content")))
			if errors.Is(err, ErrVersionConflict) {
				return nil, errors.New("version changed under you; re-read and retry")
			}
			if err != nil {
				return nil, err
			}
			return map[string]any{"ok": true, "document": updated}, nil
		},
	},
	"lock_status": {
		desc:   "Check whether a document is being written by another agent (its live lease).",
		schema: obj([]string{"id"}, map[string]any{"id": pStr}),
		fn: func(ctx context.Context, s *Server, _ string, a targs) (any, error) {
			doc, err := s.store.GetDocument(ctx, a.str("id"))
			if err != nil {
				return nil, err
			}
			lease, live, err := s.store.GetLease(ctx, doc.ID)
			if err != nil {
				return nil, err
			}
			if lease == nil || !live {
				return map[string]any{"locked": false, "lock": nil}, nil
			}
			lease.LeaseToken = "" // never leak the token to readers
			return map[string]any{"locked": true, "lock": lease}, nil
		},
	},
	"retag_doc": {
		desc:     "Change a document's tags/metadata/title WITHOUT rewriting content (no lease, version unchanged). Use namespaced tags: topic:x, kind:x, status:x.",
		mutating: true,
		schema: obj([]string{"id"}, map[string]any{"id": pStr, "add_tags": pStrs, "remove_tags": pStrs,
			"tags": pStrs, "metadata": pObj, "title": pStr}),
		fn: func(ctx context.Context, s *Server, actor string, a targs) (any, error) {
			var title *string
			if t, ok := a["title"].(string); ok {
				title = &t
			}
			doc, err := s.store.PatchDocument(ctx, a.str("id"), DocPatch{
				Tags: a.strs("tags"), AddTags: a.strs("add_tags"), RemoveTags: a.strs("remove_tags"),
				Metadata: a.raw("metadata"), Title: title,
			}, actor)
			if err != nil {
				return nil, err
			}
			return map[string]any{"document": doc}, nil
		},
	},
	"list_tags": {
		desc:   "List the whole tag vocabulary with usage counts (folio:*, topic:*, ...).",
		schema: obj(nil, map[string]any{}),
		fn: func(ctx context.Context, s *Server, _ string, a targs) (any, error) {
			tags, err := s.store.ListTags(ctx)
			if err != nil {
				return nil, err
			}
			return map[string]any{"tags": tags, "count": len(tags)}, nil
		},
	},
	"list_folios": {
		desc:   "List folios (collections of related documents, like gists). Address one by slug.",
		schema: obj(nil, map[string]any{"limit": pInt, "offset": pInt, "view": pView}),
		fn: func(ctx context.Context, s *Server, _ string, a targs) (any, error) {
			limit, offset := a.num("limit", 200), a.num("offset", 0)
			folios, total, err := s.store.ListDocuments(ctx, "", "folio", "", "", limit, offset)
			if err != nil {
				return nil, err
			}
			return docList(a.str("view"), "folios", folios, total, limit, offset), nil
		},
	},
	"create_folio": {
		desc:     "Create a folio (a collection). Add files with add_folio_file.",
		mutating: true,
		schema: obj([]string{"slug"}, map[string]any{"slug": pStr, "title": pStr,
			"description": pStr, "public": pBool}),
		fn: func(ctx context.Context, s *Server, actor string, a targs) (any, error) {
			meta := withMeta(nil, map[string]any{"description": a.str("description"), "public": a.boolean("public")})
			title := a.str("title")
			if title == "" {
				title = a.str("description")
			}
			doc, err := s.store.CreateDocument(ctx, a.str("slug"), title, "folio", nil, meta, nil, "", actor)
			if err != nil {
				return nil, err
			}
			return map[string]any{"folio": doc}, nil
		},
	},
	"get_folio": {
		desc:   "Get a folio (by slug) and the list of files it contains.",
		schema: obj([]string{"slug"}, map[string]any{"slug": pStr}),
		fn: func(ctx context.Context, s *Server, _ string, a targs) (any, error) {
			folio, err := s.store.GetDocument(ctx, a.str("slug"))
			if err != nil {
				return nil, err
			}
			files, total, err := s.store.ListDocuments(ctx, "", "", folioTag(folio.Slug), "", 500, 0)
			if err != nil {
				return nil, err
			}
			resp := docList("summary", "files", files, total, 500, 0)
			resp["folio"] = folio
			return resp, nil
		},
	},
	"get_folio_file": {
		desc:   "Get a file from a folio by folio slug + filename; optionally include content.",
		schema: obj([]string{"slug", "filename"}, map[string]any{"slug": pStr, "filename": pStr, "include_content": pBool}),
		fn: func(ctx context.Context, s *Server, _ string, a targs) (any, error) {
			doc, err := s.store.GetDocument(ctx, a.str("slug")+"/"+a.str("filename"))
			if err != nil {
				return nil, err
			}
			return s.docWithExtras(ctx, doc, a.boolean("include_content"))
		},
	},
	"add_folio_file": {
		desc:     "Add a file (document) to a folio; the folio: tag and <folio>/<filename> slug are applied server-side.",
		mutating: true,
		schema: obj([]string{"slug", "filename"}, map[string]any{"slug": pStr, "filename": pStr,
			"title": pStr, "kind": pStr, "content": pStr, "content_type": pStr, "metadata": pObj}),
		fn: func(ctx context.Context, s *Server, actor string, a targs) (any, error) {
			folio, err := s.store.GetDocument(ctx, a.str("slug"))
			if err != nil {
				return nil, err
			}
			if folio.Kind != "folio" {
				return nil, fmt.Errorf("'%s' is not a folio", a.str("slug"))
			}
			kind := a.str("kind")
			if kind == "" {
				kind = "note"
			}
			title := a.str("title")
			if title == "" {
				title = a.str("filename")
			}
			meta := withMeta(a.raw("metadata"), map[string]any{"filename": a.str("filename"), "folio": folio.Slug})
			var content []byte
			if c := a.str("content"); c != "" {
				content = []byte(c)
			}
			doc, err := s.store.CreateDocument(ctx, folio.Slug+"/"+a.str("filename"), title, kind,
				[]string{folioTag(folio.Slug)}, meta, content, a.str("content_type"), actor)
			if err != nil {
				return nil, err
			}
			return map[string]any{"document": doc}, nil
		},
	},
	"list_tasks": {
		desc:   "List tasks in the shared work queue, optionally filtered by status (open|claimed|done|failed).",
		schema: obj(nil, map[string]any{"status": pStr, "limit": pInt, "offset": pInt}),
		fn: func(ctx context.Context, s *Server, _ string, a targs) (any, error) {
			status := a.str("status")
			switch status {
			case "", "open", "claimed", "done", "failed":
			default:
				return nil, errors.New("status must be open|claimed|done|failed")
			}
			limit, offset := a.num("limit", 50), a.num("offset", 0)
			tasks, total, err := s.store.ListTasks(ctx, status, limit, offset)
			if err != nil {
				return nil, err
			}
			return map[string]any{"tasks": tasks, "count": len(tasks), "total": total, "limit": limit, "offset": offset}, nil
		},
	},
	"get_task": {
		desc:   "Get a task by id.",
		schema: obj([]string{"id"}, map[string]any{"id": pStr}),
		fn: func(ctx context.Context, s *Server, _ string, a targs) (any, error) {
			task, err := s.store.GetTask(ctx, a.str("id"))
			if err != nil {
				return nil, err
			}
			return map[string]any{"task": task}, nil
		},
	},
	"enqueue_task": {
		desc:     "Create a task in the shared work queue.",
		mutating: true,
		schema:   obj([]string{"title"}, map[string]any{"title": pStr, "payload": pObj}),
		fn: func(ctx context.Context, s *Server, actor string, a targs) (any, error) {
			task, err := s.store.CreateTask(ctx, a.str("title"), a.raw("payload"), actor)
			if err != nil {
				return nil, err
			}
			return map[string]any{"task": task}, nil
		},
	},
	"claim_task": {
		desc:     "Atomically claim the next claimable task (no two agents get the same one; expired claims are re-claimable). Returns {claimed:false} when the queue is empty.",
		mutating: true,
		schema:   obj(nil, map[string]any{}),
		fn: func(ctx context.Context, s *Server, actor string, a targs) (any, error) {
			task, err := s.store.ClaimNextTask(ctx, actor, s.cfg.TaskClaimTTL)
			if errors.Is(err, ErrNotFound) {
				return map[string]any{"claimed": false, "task": nil}, nil
			}
			if err != nil {
				return nil, err
			}
			return map[string]any{"claimed": true, "task": task}, nil
		},
	},
	"complete_task": {
		desc:     "Mark a task done|failed with an optional result. Caller must be the current claimant.",
		mutating: true,
		schema: obj([]string{"id"}, map[string]any{"id": pStr,
			"status": map[string]any{"type": "string", "enum": []string{"done", "failed"}}, "result": pObj}),
		fn: func(ctx context.Context, s *Server, actor string, a targs) (any, error) {
			status := a.str("status")
			if status == "" {
				status = "done"
			}
			task, err := s.store.CompleteTask(ctx, a.str("id"), status, a.raw("result"), actor)
			if err != nil {
				return nil, err
			}
			return map[string]any{"task": task}, nil
		},
	},
	"list_actors": {
		desc:   "List entities (agents/humans) that have acted on the store, with last_seen and action_count.",
		schema: obj(nil, map[string]any{}),
		fn: func(ctx context.Context, s *Server, _ string, a targs) (any, error) {
			actors, err := s.store.ListActors(ctx)
			if err != nil {
				return nil, err
			}
			return map[string]any{"actors": actors, "count": len(actors)}, nil
		},
	},
	"actor_activity": {
		desc:   "An entity's most recent document writes, from the revision log.",
		schema: obj([]string{"name"}, map[string]any{"name": pStr, "limit": pInt}),
		fn: func(ctx context.Context, s *Server, _ string, a targs) (any, error) {
			limit := a.num("limit", 50)
			if limit <= 0 || limit > 500 {
				limit = 50
			}
			items, err := s.store.ActorActivity(ctx, a.str("name"), limit)
			if err != nil {
				return nil, err
			}
			return map[string]any{"activity": items, "count": len(items)}, nil
		},
	},
}
