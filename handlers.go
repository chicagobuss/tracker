package main

import (
	"encoding/json"
	"errors"
	"io"
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
	docs, total, err := s.store.ListDocuments(r.Context(), q.Get("q"), q.Get("kind"), q.Get("tag"), limit, offset)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"documents": docs, "count": len(docs), "total": total, "limit": limit, "offset": offset,
	})
}

func (s *Server) getDoc(w http.ResponseWriter, r *http.Request) {
	doc, err := s.store.GetDocument(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.docEnvelope(r, doc))
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
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		badRequest(w, "could not read body")
		return
	}
	ctype := r.Header.Get("Content-Type")
	if ctype == "" {
		ctype = "text/markdown"
	}
	doc, err := s.store.WriteContent(r.Context(), r.PathValue("id"), owner, token, baseVersion, ctype, body)
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

// --- folios (collections; a folio is a kind=folio doc, members carry folio:<slug>) ---

func folioTag(slug string) string { return "folio:" + slug }

func (s *Server) listFolios(w http.ResponseWriter, r *http.Request) {
	folios, total, err := s.store.ListDocuments(r.Context(), "", "folio", "", 500, 0)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"folios": folios, "count": len(folios), "total": total})
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
	files, total, err := s.store.ListDocuments(r.Context(), "", "", folioTag(folio.Slug), 500, 0)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"folio": folio, "files": files, "count": len(files), "total": total})
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
	lease, err := s.store.AcquireLease(r.Context(), r.PathValue("id"), owner, in.Reason, ttl, in.LeaseToken)
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
	lease, live, err := s.store.GetLease(r.Context(), r.PathValue("id"))
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
	token := r.Header.Get("X-Lease-Token")
	if token == "" {
		badRequest(w, "X-Lease-Token header required")
		return
	}
	if err := s.store.ReleaseLease(r.Context(), r.PathValue("id"), token); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"released": true})
}

// --- tasks ---

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
	task, err := s.store.ClaimNextTask(r.Context(), worker)
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
