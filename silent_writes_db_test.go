package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// End-to-end coverage for the three "answered 200, did nothing" defects. These
// go through the real mux and the auth middleware, so they exercise routing,
// workspace scoping and content delivery rather than the helpers in isolation.

func silentWritesMux(srv *Server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /docs", srv.auth(srv.listDocs))
	mux.HandleFunc("GET /docs/{id}", srv.auth(srv.getDoc))
	mux.HandleFunc("GET /docs/{rest...}", srv.auth(srv.getDoc))
	mux.HandleFunc("GET /docs/{id}/revisions", srv.auth(srv.listRevisions))
	mux.HandleFunc("GET /docs/{id}/revisions/{version}/raw", srv.auth(srv.rawRevision))
	mux.HandleFunc("PATCH /docs/{id}", srv.auth(srv.patchDoc))
	mux.HandleFunc("PATCH /docs/{rest...}", srv.auth(srv.patchDoc))
	return mux
}

func do(t *testing.T, mux *http.ServeMux, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	}
	r.Header.Set("X-Actor", "tester")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

// PATCH must refuse a content field instead of applying the tags, dropping the
// content and answering 200 with a content hash for the unchanged bytes.
func TestPatchRefusesContent(t *testing.T) {
	s := testStore(t)
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}
	mux := silentWritesMux(srv)

	ctx, release, err := s.Scoped(context.Background(), "default")
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	ctx = context.WithValue(ctx, reqWSKey{}, reqWS{name: "default"})
	if _, err := s.CreateDocument(ctx, "patch-probe", "probe", "note", nil, nil,
		[]byte("ORIGINAL"), "text/markdown", "tester"); err != nil {
		release()
		t.Fatalf("create: %v", err)
	}
	release()

	for _, body := range []string{
		`{"content":"REPLACED","tags":["probe"]}`,
		`{"content_type":"text/plain"}`,
		// One decode reads one value; a trailing object must not slip past.
		`{"add_tags":["x"]} {"content":"REPLACED"}`,
	} {
		if w := do(t, mux, "PATCH", "/docs/patch-probe", body); w.Code != http.StatusBadRequest {
			t.Fatalf("PATCH %s = %d, want 400 (body %s)", body, w.Code, w.Body.String())
		}
	}

	// The document must be untouched by every rejected call.
	w := do(t, mux, "GET", "/docs/patch-probe", "")
	var env struct {
		Document struct {
			Version int      `json:"version"`
			Tags    []string `json:"tags"`
		} `json:"document"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Document.Version != 1 {
		t.Fatalf("version = %d, want 1 — a refused PATCH changed the document", env.Document.Version)
	}
	if len(env.Document.Tags) != 0 {
		t.Fatalf("tags = %v, want none — a refused PATCH applied its tags anyway", env.Document.Tags)
	}

	// A legitimate relabel still works.
	if w := do(t, mux, "PATCH", "/docs/patch-probe", `{"add_tags":["kept"]}`); w.Code != http.StatusOK {
		t.Fatalf("legitimate relabel = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
}

// ?workspace= must select, not be accepted and ignored.
func TestListDocsHonoursWorkspaceQuery(t *testing.T) {
	s := testStore(t)
	base := context.Background()
	if _, err := s.CreateWorkspace(base, "wsq", "test workspace", "tester"); err != nil &&
		err.Error() != ErrAlreadyExists.Error() {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	for ws, slug := range map[string]string{"default": "in-default", "wsq": "in-wsq"} {
		ctx, release, err := s.Scoped(base, ws)
		if err != nil {
			t.Fatalf("scope %s: %v", ws, err)
		}
		ctx = context.WithValue(ctx, reqWSKey{}, reqWS{name: ws})
		if _, err := s.CreateDocument(ctx, slug, slug, "note", nil, nil, nil, "", "tester"); err != nil {
			release()
			t.Fatalf("create %s: %v", slug, err)
		}
		release()
	}

	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}
	mux := silentWritesMux(srv)

	var out struct {
		Documents []struct {
			Slug string `json:"slug"`
		} `json:"documents"`
	}
	w := do(t, mux, "GET", "/docs?workspace=wsq", "")
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Documents) != 1 || out.Documents[0].Slug != "in-wsq" {
		t.Fatalf("?workspace=wsq returned %+v — the query was ignored and the default "+
			"workspace answered with 200", out.Documents)
	}
}

// A folio-style slug must reach its own revisions and their bytes.
func TestRevisionSubRoutesByMultiSegmentSlug(t *testing.T) {
	s := testStore(t)
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}
	mux := silentWritesMux(srv)

	ctx, release, err := s.Scoped(context.Background(), "default")
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	ctx = context.WithValue(ctx, reqWSKey{}, reqWS{name: "default"})
	const slug = "myfolio/file.md"
	doc, err := s.CreateDocument(ctx, slug, "file", "note", nil, nil,
		[]byte("VERSION ONE"), "text/markdown", "tester")
	if err != nil {
		release()
		t.Fatalf("create: %v", err)
	}
	lease, err := s.AcquireLease(ctx, slug, "tester", "test", 60_000_000_000, "")
	if err != nil {
		release()
		t.Fatalf("lease: %v", err)
	}
	if _, err := s.WriteContent(ctx, slug, "tester", lease.LeaseToken, doc.Version,
		"text/markdown", []byte("VERSION TWO")); err != nil {
		release()
		t.Fatalf("write v2: %v", err)
	}
	release()

	w := do(t, mux, "GET", "/docs/"+slug+"/revisions", "")
	if w.Code != http.StatusOK {
		t.Fatalf("/revisions by slug = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	var revs struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &revs); err != nil || revs.Count < 2 {
		t.Fatalf("revisions count = %d, want >= 2 (err %v)", revs.Count, err)
	}

	// The old bytes, not the current ones — a plain "/raw" fallback would
	// silently serve VERSION TWO here.
	w = do(t, mux, "GET", "/docs/"+slug+"/revisions/1/raw", "")
	if w.Code != http.StatusOK {
		t.Fatalf("/revisions/1/raw = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	got, _ := io.ReadAll(w.Body)
	if string(got) != "VERSION ONE" {
		t.Fatalf("revision 1 raw = %q, want %q", got, "VERSION ONE")
	}

	// The plain content route must still serve the current bytes.
	w = do(t, mux, "GET", "/docs/"+slug+"/raw", "")
	got, _ = io.ReadAll(w.Body)
	if string(got) != "VERSION TWO" {
		t.Fatalf("current raw = %q, want %q", got, "VERSION TWO")
	}
}
