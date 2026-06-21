package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
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

func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
	case errors.Is(err, ErrLeaseHeld):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	case errors.Is(err, ErrNoLease):
		writeJSON(w, http.StatusLocked, map[string]string{"error": err.Error()})
	case errors.Is(err, ErrVersionConflict):
		writeJSON(w, http.StatusPreconditionFailed, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
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
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid or missing bearer token"})
				return
			}
		}
		h(w, r)
	}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- documents ---

func (s *Server) createDoc(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Slug  string   `json:"slug"`
		Title string   `json:"title"`
		Kind  string   `json:"kind"`
		Tags  []string `json:"tags"`
		By    string   `json:"by"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Slug == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "json body with non-empty 'slug' required"})
		return
	}
	doc, err := s.store.CreateDocument(r.Context(), in.Slug, in.Title, in.Kind, in.Tags, in.By)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, doc)
}

func (s *Server) listDocs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	docs, err := s.store.ListDocuments(r.Context(), q.Get("q"), q.Get("kind"), q.Get("tag"))
	if err != nil {
		writeErr(w, err)
		return
	}
	if docs == nil {
		docs = []Document{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"documents": docs})
}

func (s *Server) getDoc(w http.ResponseWriter, r *http.Request) {
	doc, err := s.store.GetDocument(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	resp := map[string]any{"document": doc}
	if doc.ContentKey != "" {
		if u, err := s.store.PresignGet(r.Context(), doc.ContentKey, 15*time.Minute); err == nil {
			resp["content_url"] = u
		}
	}
	// Surface live-lock state so a reader can see if someone's mid-write.
	if l, live, err := s.store.GetLease(r.Context(), doc.ID); err == nil && live {
		resp["locked_by"] = l.Owner
		resp["locked_until"] = l.ExpiresAt
	}
	writeJSON(w, http.StatusOK, resp)
}

// putDoc writes content. Requires headers:
//
//	X-Owner:       agent id (must match lease owner)
//	X-Lease-Token: the lease token from POST /docs/{id}/lock
//	If-Match:      the document version you based this write on (optimistic CAS)
func (s *Server) putDoc(w http.ResponseWriter, r *http.Request) {
	owner := r.Header.Get("X-Owner")
	token := r.Header.Get("X-Lease-Token")
	baseVersion, err := strconv.Atoi(r.Header.Get("If-Match"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "If-Match header must be the integer base version"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20)) // 64 MiB cap
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "could not read body"})
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
	writeJSON(w, http.StatusOK, doc)
}

// --- locks (leases) ---

func (s *Server) acquireLock(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Owner      string `json:"owner"`
		Reason     string `json:"reason"`
		TTLSeconds int    `json:"ttl_seconds"`
		LeaseToken string `json:"lease_token"` // present => renew
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Owner == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "json body with non-empty 'owner' required"})
		return
	}
	ttl := time.Duration(in.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	lease, err := s.store.AcquireLease(r.Context(), r.PathValue("id"), in.Owner, in.Reason, ttl, in.LeaseToken)
	if errors.Is(err, ErrLeaseHeld) {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "locked", "held_by": lease.Owner, "expires_at": lease.ExpiresAt})
		return
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, lease)
}

func (s *Server) getLock(w http.ResponseWriter, r *http.Request) {
	lease, live, err := s.store.GetLease(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	if lease == nil || !live {
		writeJSON(w, http.StatusOK, map[string]any{"locked": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"locked": true, "lease": lease})
}

func (s *Server) releaseLock(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("X-Lease-Token")
	if token == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "X-Lease-Token header required"})
		return
	}
	if err := s.store.ReleaseLease(r.Context(), r.PathValue("id"), token); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "released"})
}

// --- tasks ---

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Title   string          `json:"title"`
		Payload json.RawMessage `json:"payload"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	task, err := s.store.CreateTask(r.Context(), in.Title, in.Payload)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, task)
}

func (s *Server) claimTask(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Worker string `json:"worker"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Worker == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "json body with non-empty 'worker' required"})
		return
	}
	task, err := s.store.ClaimNextTask(r.Context(), in.Worker)
	if errors.Is(err, ErrNotFound) {
		writeJSON(w, http.StatusNoContent, nil)
		return
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) completeTask(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Status string          `json:"status"`
		Result json.RawMessage `json:"result"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	task, err := s.store.CompleteTask(r.Context(), r.PathValue("id"), in.Status, in.Result)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, task)
}
