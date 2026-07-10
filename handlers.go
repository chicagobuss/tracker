package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	store *Store
	cfg   Config
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError emits the uniform error envelope: {"error": {"code", "message", ...}}.
func writeError(w http.ResponseWriter, status int, code, message string, extra map[string]any) {
	e := map[string]any{"code": code, "message": message}
	for k, v := range extra {
		e[k] = v
	}
	writeJSON(w, status, map[string]any{"error": e})
}

// writeErr maps a store sentinel error to its status + machine code.
func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error(), nil)
	case errors.Is(err, ErrLeaseHeld):
		writeError(w, http.StatusConflict, "lease_held", err.Error(), nil)
	case errors.Is(err, ErrNoLease):
		writeError(w, http.StatusLocked, "no_lease", err.Error(), nil)
	case errors.Is(err, ErrVersionConflict):
		writeError(w, http.StatusPreconditionFailed, "version_conflict", err.Error(), nil)
	case errors.Is(err, ErrBadTaskStatus):
		writeError(w, http.StatusBadRequest, "bad_request", err.Error(), nil)
	case errors.Is(err, ErrNotClaimant):
		writeError(w, http.StatusConflict, "not_claimant", err.Error(), nil)
	case errors.Is(err, ErrNotClaimable):
		writeError(w, http.StatusConflict, "not_claimable", err.Error(), nil)
	default:
		writeError(w, http.StatusInternalServerError, "internal", err.Error(), nil)
	}
}

// auth wraps a handler with bearer-token checking. If no tokens are configured
// (API_TOKENS empty), auth is disabled (dev only).
func (s *Server) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(s.cfg.APITokens) > 0 {
			tok := r.Header.Get("Authorization")
			const p = "Bearer "
			if len(tok) <= len(p) || tok[:len(p)] != p || !s.cfg.APITokens[tok[len(p):]] {
				writeError(w, http.StatusUnauthorized, "unauthorized", "invalid or missing bearer token", nil)
				return
			}
		}
		h(w, r)
	}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.db.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "unhealthy", "postgres unreachable", map[string]any{"version": appVersion()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "version": appVersion()})
}

func (s *Server) versionInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"version": appVersion()})
}

// actor returns the entity performing a mutating request, from the required
// X-Actor header. On a missing/blank value it writes a 400 and returns false.
func (s *Server) actor(w http.ResponseWriter, r *http.Request) (string, bool) {
	a := strings.TrimSpace(r.Header.Get("X-Actor"))
	if a == "" {
		writeError(w, http.StatusBadRequest, "actor_required",
			"X-Actor header required (the entity performing this action)", nil)
		return "", false
	}
	return a, true
}

// pageParams reads ?limit=&offset= with sane defaults/caps.
func pageParams(r *http.Request) (limit, offset int) {
	limit, offset = 50, 0
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 {
		limit = n
	}
	if limit > 200 {
		limit = 200
	}
	if n, err := strconv.Atoi(r.URL.Query().Get("offset")); err == nil && n > 0 {
		offset = n
	}
	return
}

func badRequest(w http.ResponseWriter, msg string) {
	writeError(w, http.StatusBadRequest, "bad_request", msg, nil)
}

// withMeta merges add into a (possibly empty) metadata blob.
func withMeta(raw json.RawMessage, add map[string]any) json.RawMessage {
	m := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &m)
	}
	for k, v := range add {
		m[k] = v
	}
	b, _ := json.Marshal(m)
	return b
}

// docID reads the document id/slug from the route, accepting both the single
// segment {id} and the multi-segment {rest...} wildcard (so folio-file slugs like
// "myfolio/file.md", which contain a '/', resolve at /docs/...).
func docID(r *http.Request) string {
	if id := r.PathValue("id"); id != "" {
		return id
	}
	return r.PathValue("rest")
}

// --- list views (the agent plane: token-shaped projections of a doc list) ---
//
// view=summary (default) trims each doc to the fields you browse by; view=table
// is a columnar {cols, rows} shape that emits each key once instead of per row
// (≈10× smaller for a big list); view=full returns whole Document objects.

var tableCols = []string{"slug", "title", "kind", "tags", "kb", "v", "age"}

func kb(b int64) float64 { return math.Round(float64(b)/1024.0*10) / 10 }

// age renders a compact relative timestamp ("5h", "2d") for the dense table view.
func age(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "now"
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

func summaryView(docs []Document) []map[string]any {
	out := make([]map[string]any, len(docs))
	for i, d := range docs {
		out[i] = map[string]any{
			"id": d.ID, "slug": d.Slug, "title": d.Title, "kind": d.Kind,
			"tags": d.Tags, "size_bytes": d.SizeBytes, "version": d.Version,
			"updated_at": d.UpdatedAt,
		}
	}
	return out
}

func tableView(docs []Document) map[string]any {
	rows := make([][]any, len(docs))
	for i, d := range docs {
		rows[i] = []any{d.Slug, d.Title, d.Kind, strings.Join(d.Tags, ","), kb(d.SizeBytes), d.Version, age(d.UpdatedAt)}
	}
	return map[string]any{"cols": tableCols, "rows": rows}
}

// listView renders docs according to ?view, using listKey ("documents"/"folios"/
// "files") for the object-bearing modes. The caller adds count/total/paging.
func listView(r *http.Request, listKey string, docs []Document) map[string]any {
	switch r.URL.Query().Get("view") {
	case "full":
		return map[string]any{listKey: docs}
	case "table":
		return tableView(docs)
	default: // summary
		return map[string]any{listKey: summaryView(docs)}
	}
}

// docEnvelope is the canonical single-document response: the document plus a
// short-lived content URL and live lock state (null when unlocked).
func (s *Server) docEnvelope(r *http.Request, doc *Document) map[string]any {
	resp := map[string]any{"document": doc, "content_url": nil, "lock": nil}
	if doc.ContentKey != "" {
		if u, err := s.store.PresignGet(r.Context(), doc.ContentKey, 15*time.Minute); err == nil {
			resp["content_url"] = u
		}
	}
	if l, live, err := s.store.GetLease(r.Context(), doc.ID); err == nil && live {
		resp["lock"] = map[string]any{"owner": l.Owner, "expires_at": l.ExpiresAt}
	}
	return resp
}

// --- documents ---

func (s *Server) createDoc(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	var in struct {
		Slug        string          `json:"slug"`
		Title       string          `json:"title"`
		Kind        string          `json:"kind"`
		Tags        []string        `json:"tags"`
		Metadata    json.RawMessage `json:"metadata"`
		Content     string          `json:"content"`
		ContentType string          `json:"content_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Slug == "" {
		badRequest(w, "json body with non-empty 'slug' required")
		return
	}
	var content []byte
	if in.Content != "" {
		content = []byte(in.Content)
	}
	doc, err := s.store.CreateDocument(r.Context(), in.Slug, in.Title, in.Kind, in.Tags, in.Metadata, content, in.ContentType, actor)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"document": doc})
}

func (s *Server) listDocs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, offset := pageParams(r)
	query := q.Get("q")
	docs, total, err := s.store.ListDocuments(r.Context(), query, q.Get("kind"), q.Get("tag"), q.Get("mode"), limit, offset)
	if err != nil {
		writeErr(w, err)
		return
	}
	resp := listView(r, "documents", docs)
	resp["count"], resp["total"], resp["limit"], resp["offset"] = len(docs), total, limit, offset
	// Don't let a silently-AND'd multi-word query look like an empty store.
	if total == 0 && len(strings.Fields(query)) > 1 && q.Get("mode") != "plain" {
		resp["hint"] = `0 results — multi-word queries match ALL terms by default. Try fewer words, "a quoted phrase", or "a OR b".`
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getDoc(w http.ResponseWriter, r *http.Request) {
	doc, err := s.store.GetDocument(r.Context(), docID(r))
	if errors.Is(err, ErrNotFound) {
		// Multi-segment fallbacks: an exact slug always wins, but when nothing
		// matches, ".../raw" streams the prefix's bytes and ".../lock" reports
		// its lease — so folio-file slugs get the same sub-routes as {id}.
		if rest := r.PathValue("rest"); rest != "" {
			if base, ok := strings.CutSuffix(rest, "/raw"); ok {
				s.streamContent(w, r, base)
				return
			}
			if base, ok := strings.CutSuffix(rest, "/lock"); ok {
				s.lockStatus(w, r, base)
				return
			}
		}
		writeError(w, http.StatusNotFound, "not_found", "no document with that id or slug",
			map[string]any{"hint": "folio files are addressable by full slug (/docs/myfolio/file.md) or at /folios/{slug}/files/{filename}"})
		return
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.docEnvelope(r, doc))
}

// patchDoc mutates labels only — tags and/or metadata (and optionally title) —
// without rewriting content. No lease and no version bump: tags/metadata are
// labels, not content. Requires X-Actor for attribution.
func (s *Server) patchDoc(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	var in struct {
		Tags       []string        `json:"tags"`
		AddTags    []string        `json:"add_tags"`
		RemoveTags []string        `json:"remove_tags"`
		Metadata   json.RawMessage `json:"metadata"`
		Title      *string         `json:"title"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		badRequest(w, "json body required (tags / add_tags / remove_tags / metadata / title)")
		return
	}
	doc, err := s.store.PatchDocument(r.Context(), docID(r), DocPatch{
		Tags: in.Tags, AddTags: in.AddTags, RemoveTags: in.RemoveTags,
		Metadata: in.Metadata, Title: in.Title,
	}, actor)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.docEnvelope(r, doc))
}

// listTags returns the whole tag vocabulary with counts.
func (s *Server) listTags(w http.ResponseWriter, r *http.Request) {
	tags, err := s.store.ListTags(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tags": tags, "count": len(tags)})
}

// putDoc writes content. Headers: X-Actor (must hold the lease), X-Lease-Token,
// If-Match (the integer base version, optimistic CAS).
func (s *Server) putDoc(w http.ResponseWriter, r *http.Request) {
	owner, ok := s.actor(w, r)
	if !ok {
		return
	}
	token := r.Header.Get("X-Lease-Token")
	baseVersion, err := strconv.Atoi(r.Header.Get("If-Match"))
	if err != nil {
		badRequest(w, "If-Match header must be the integer base version")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "request_too_large",
				fmt.Sprintf("body exceeds the %d-byte limit", mbe.Limit), nil)
			return
		}
		badRequest(w, "could not read body")
		return
	}
	ctype := r.Header.Get("Content-Type")
	if ctype == "" {
		ctype = "text/markdown"
	}
	doc, err := s.store.WriteContent(r.Context(), docID(r), owner, token, baseVersion, ctype, body)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"document": doc})
}

// rawDoc streams a document's content bytes (same-origin; the UI uses this).
func (s *Server) rawDoc(w http.ResponseWriter, r *http.Request) {
	s.streamContent(w, r, r.PathValue("id"))
}

func (s *Server) streamContent(w http.ResponseWriter, r *http.Request, idOrSlug string) {
	doc, err := s.store.GetDocument(r.Context(), idOrSlug)
	if err != nil {
		writeErr(w, err)
		return
	}
	if doc.ContentKey == "" {
		writeError(w, http.StatusNotFound, "no_content", "document has no content yet", nil)
		return
	}
	rc, err := s.store.GetContent(r.Context(), doc.ContentKey)
	if err != nil {
		writeErr(w, err)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", doc.ContentType)
	_, _ = io.Copy(w, rc)
}

// listRevisions returns a document's version history (newest first).
func (s *Server) listRevisions(w http.ResponseWriter, r *http.Request) {
	revs, err := s.store.DocRevisions(r.Context(), docID(r))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revisions": revs, "count": len(revs)})
}

// rawRevision streams the content bytes of a specific past version.
func (s *Server) rawRevision(w http.ResponseWriter, r *http.Request) {
	version, err := strconv.Atoi(r.PathValue("version"))
	if err != nil {
		badRequest(w, "version must be an integer")
		return
	}
	key, ctype, err := s.store.RevisionContent(r.Context(), docID(r), version)
	if err != nil {
		writeErr(w, err)
		return
	}
	if key == "" {
		writeError(w, http.StatusNotFound, "no_content", "that revision has no content", nil)
		return
	}
	rc, err := s.store.GetContent(r.Context(), key)
	if err != nil {
		writeErr(w, err)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", ctype)
	_, _ = io.Copy(w, rc)
}

// --- folios (collections; a folio is a kind=folio doc, members carry folio:<slug>) ---

func folioTag(slug string) string { return "folio:" + slug }

func (s *Server) listFolios(w http.ResponseWriter, r *http.Request) {
	folios, total, err := s.store.ListDocuments(r.Context(), "", "folio", "", "", 500, 0)
	if err != nil {
		writeErr(w, err)
		return
	}
	resp := listView(r, "folios", folios)
	resp["count"], resp["total"] = len(folios), total
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) createFolio(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	var in struct {
		Slug        string `json:"slug"`
		Title       string `json:"title"`
		Description string `json:"description"`
		Public      bool   `json:"public"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Slug == "" {
		badRequest(w, "json body with non-empty 'slug' required")
		return
	}
	meta := withMeta(nil, map[string]any{"description": in.Description, "public": in.Public})
	title := in.Title
	if title == "" {
		title = in.Description
	}
	doc, err := s.store.CreateDocument(r.Context(), in.Slug, title, "folio", nil, meta, nil, "", actor)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"folio": doc})
}

func (s *Server) getFolio(w http.ResponseWriter, r *http.Request) {
	folio, err := s.store.GetDocument(r.Context(), r.PathValue("slug"))
	if err != nil {
		writeErr(w, err)
		return
	}
	files, total, err := s.store.ListDocuments(r.Context(), "", "", folioTag(folio.Slug), "", 500, 0)
	if err != nil {
		writeErr(w, err)
		return
	}
	resp := listView(r, "files", files)
	resp["folio"], resp["count"], resp["total"] = folio, len(files), total
	writeJSON(w, http.StatusOK, resp)
}

// createFolioFile adds a document to a folio, applying the folio: tag and the
// <folio>/<filename> slug convention server-side, so callers don't have to know it.
func (s *Server) createFolioFile(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	folioSlug := r.PathValue("slug")
	folio, err := s.store.GetDocument(r.Context(), folioSlug)
	if err != nil {
		writeErr(w, err)
		return
	}
	if folio.Kind != "folio" {
		writeError(w, http.StatusBadRequest, "not_a_folio", "'"+folioSlug+"' is not a folio", nil)
		return
	}
	var in struct {
		Filename    string          `json:"filename"`
		Title       string          `json:"title"`
		Kind        string          `json:"kind"`
		Content     string          `json:"content"`
		ContentType string          `json:"content_type"`
		Metadata    json.RawMessage `json:"metadata"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Filename == "" {
		badRequest(w, "json body with non-empty 'filename' required")
		return
	}
	kind := in.Kind
	if kind == "" {
		kind = "note"
	}
	title := in.Title
	if title == "" {
		title = in.Filename
	}
	meta := withMeta(in.Metadata, map[string]any{"filename": in.Filename, "folio": folio.Slug})
	var content []byte
	if in.Content != "" {
		content = []byte(in.Content)
	}
	doc, err := s.store.CreateDocument(r.Context(), folio.Slug+"/"+in.Filename, title, kind,
		[]string{folioTag(folio.Slug)}, meta, content, in.ContentType, actor)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"document": doc})
}

// getFolioFile addresses a folio member by name (so files whose slug contains a
// '/' are reachable without their UUID).
func (s *Server) getFolioFile(w http.ResponseWriter, r *http.Request) {
	doc, err := s.store.GetDocument(r.Context(), r.PathValue("slug")+"/"+r.PathValue("filename"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.docEnvelope(r, doc))
}

func (s *Server) rawFolioFile(w http.ResponseWriter, r *http.Request) {
	s.streamContent(w, r, r.PathValue("slug")+"/"+r.PathValue("filename"))
}

// --- locks (leases) ---

func (s *Server) acquireLock(w http.ResponseWriter, r *http.Request) {
	s.acquireLockID(w, r, r.PathValue("id"))
}

func (s *Server) acquireLockID(w http.ResponseWriter, r *http.Request, id string) {
	owner, ok := s.actor(w, r)
	if !ok {
		return
	}
	var in struct {
		Reason     string `json:"reason"`
		TTLSeconds int    `json:"ttl_seconds"`
		LeaseToken string `json:"lease_token"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	ttl := time.Duration(in.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	lease, err := s.store.AcquireLease(r.Context(), id, owner, in.Reason, ttl, in.LeaseToken)
	if errors.Is(err, ErrLeaseHeld) {
		writeError(w, http.StatusConflict, "lease_held", "document is locked by another actor",
			map[string]any{"held_by": lease.Owner, "expires_at": lease.ExpiresAt})
		return
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lock": lease})
}

func (s *Server) getLock(w http.ResponseWriter, r *http.Request) {
	s.lockStatus(w, r, r.PathValue("id"))
}

func (s *Server) lockStatus(w http.ResponseWriter, r *http.Request, id string) {
	lease, live, err := s.store.GetLease(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	if lease == nil || !live {
		writeJSON(w, http.StatusOK, map[string]any{"locked": false, "lock": nil})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"locked": true, "lock": lease})
}

func (s *Server) releaseLock(w http.ResponseWriter, r *http.Request) {
	s.releaseLockID(w, r, r.PathValue("id"))
}

func (s *Server) releaseLockID(w http.ResponseWriter, r *http.Request, id string) {
	token := r.Header.Get("X-Lease-Token")
	if token == "" {
		badRequest(w, "X-Lease-Token header required")
		return
	}
	if err := s.store.ReleaseLease(r.Context(), id, token); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"released": true})
}

// lockDocRest routes POST/DELETE /docs/{rest...} — the wildcard can only sit at
// the end of a pattern, so ".../lock" on a multi-segment slug is dispatched by
// suffix here instead of by a route.
func (s *Server) lockDocRest(w http.ResponseWriter, r *http.Request) {
	base, ok := strings.CutSuffix(r.PathValue("rest"), "/lock")
	if !ok {
		writeError(w, http.StatusNotFound, "not_found",
			"unknown action — use /docs/{slug}/lock to acquire or release a lease", nil)
		return
	}
	if r.Method == http.MethodDelete {
		s.releaseLockID(w, r, base)
		return
	}
	s.acquireLockID(w, r, base)
}

// --- tasks ---

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	if status != "" && status != "open" && status != "claimed" && status != "done" && status != "failed" {
		badRequest(w, "invalid status")
		return
	}
	limit, offset := pageParams(r)
	tasks, total, err := s.store.ListTasks(r.Context(), status, limit, offset)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tasks":  tasks,
		"count":  len(tasks),
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	task, err := s.store.GetTask(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"task": task})
}

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	var in struct {
		Title   string          `json:"title"`
		Payload json.RawMessage `json:"payload"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	task, err := s.store.CreateTask(r.Context(), in.Title, in.Payload, actor)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"task": task})
}

func (s *Server) claimTask(w http.ResponseWriter, r *http.Request) {
	worker, ok := s.actor(w, r)
	if !ok {
		return
	}
	task, err := s.store.ClaimNextTask(r.Context(), worker, s.cfg.TaskClaimTTL)
	if errors.Is(err, ErrNotFound) {
		writeJSON(w, http.StatusOK, map[string]any{"task": nil})
		return
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"task": task})
}

// claimTaskByID claims one specific task (vs. /tasks/claim's FIFO next).
func (s *Server) claimTaskByID(w http.ResponseWriter, r *http.Request) {
	worker, ok := s.actor(w, r)
	if !ok {
		return
	}
	task, err := s.store.ClaimTask(r.Context(), r.PathValue("id"), worker, s.cfg.TaskClaimTTL)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"task": task})
}

func (s *Server) completeTask(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	var in struct {
		Status string          `json:"status"`
		Result json.RawMessage `json:"result"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	task, err := s.store.CompleteTask(r.Context(), r.PathValue("id"), in.Status, in.Result, actor)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"task": task})
}

// --- actors ---

func (s *Server) listActors(w http.ResponseWriter, r *http.Request) {
	actors, err := s.store.ListActors(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"actors": actors, "count": len(actors)})
}

func (s *Server) actorActivity(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 && n <= 500 {
		limit = n
	}
	items, err := s.store.ActorActivity(r.Context(), r.PathValue("name"), limit)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"activity": items, "count": len(items)})
}
