package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestWriteErr_AlreadyExists(t *testing.T) {
	w := httptest.NewRecorder()
	writeErr(w, ErrAlreadyExists)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusConflict)
	}
	if got := w.Body.String(); got != "{\"error\":{\"code\":\"already_exists\",\"message\":\"already exists\"}}\n" {
		t.Errorf("body = %s", got)
	}
}

func TestNormalizeCreateError_UniqueViolation(t *testing.T) {
	err := normalizeCreateError(&pgconn.PgError{Code: "23505"})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("err = %v, want ErrAlreadyExists", err)
	}
}

// Tags handed to a folio-file create used to have nowhere to go: the endpoint
// took only metadata, so callers put them there and they silently stopped being
// searchable — metadata is not matched by the tag filter. folioTags is the merge
// that create now runs, so pin its shape: membership leads, the caller's tags
// follow, and the union dedupes.
func TestFolioTags(t *testing.T) {
	cases := []struct {
		name string
		slug string
		tags []string
		want []string
	}{
		{"no caller tags", "beta", nil, []string{"folio:beta"}},
		{"empty slice", "beta", []string{}, []string{"folio:beta"}},
		{"membership leads", "beta", []string{"course:beta", "status:active"},
			[]string{"folio:beta", "course:beta", "status:active"}},
		{"caller repeats the folio tag", "beta", []string{"folio:beta", "course:beta"},
			[]string{"folio:beta", "course:beta"}},
		{"duplicates collapse", "beta", []string{"course:beta", "course:beta"},
			[]string{"folio:beta", "course:beta"}},
		{"empty tags dropped", "beta", []string{"", "course:beta"},
			[]string{"folio:beta", "course:beta"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := folioTags(tc.slug, tc.tags)
			if len(got) != len(tc.want) {
				t.Fatalf("folioTags(%q, %v) = %v, want %v", tc.slug, tc.tags, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("folioTags(%q, %v) = %v, want %v", tc.slug, tc.tags, got, tc.want)
				}
			}
		})
	}
}

// The folio tag is what folio membership *is*, so a caller must not be able to
// displace it by passing tags of their own.
func TestFolioTags_MembershipSurvivesCallerTags(t *testing.T) {
	got := folioTags("beta", []string{"course:beta", "audience:agents"})
	if got[0] != "folio:beta" {
		t.Fatalf("membership tag = %q, want folio:beta (got %v)", got[0], got)
	}
}

func TestChanges_NormalizeKinds(t *testing.T) {
	kinds := normalizeKinds([]string{"doc_created,doc_updated", " task_enqueued "})
	wantKinds := []string{"doc_created", "doc_updated", "task_enqueued"}
	if len(kinds) != len(wantKinds) {
		t.Fatalf("parseKinds got %v, want %v", kinds, wantKinds)
	}
	for i := range kinds {
		if kinds[i] != wantKinds[i] {
			t.Errorf("kinds[%d] = %q, want %q", i, kinds[i], wantKinds[i])
		}
	}
}

type testChangesResponse struct {
	Count      int     `json:"count"`
	Events     []Event `json:"events"`
	NextCursor string  `json:"next_cursor"`
}

func callListChanges(t *testing.T, srv *Server, ctx context.Context, target string) (testChangesResponse, *httptest.ResponseRecorder) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
	w := httptest.NewRecorder()
	srv.listChanges(w, req)
	var body testChangesResponse
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode %s: %v (body %s)", target, err, w.Body.String())
		}
	}
	return body, w
}

func TestChanges_PagingAndWorkspaceScoping(t *testing.T) {
	s := testStore(t)
	base := context.Background()
	if _, err := s.CreateWorkspace(base, "alpha", "test workspace", "tester"); err != nil && !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("CreateWorkspace: %v", err)
	}

	defaultCtx, defaultRelease, err := s.Scoped(base, "default")
	if err != nil {
		t.Fatalf("scope default: %v", err)
	}
	defer defaultRelease()
	defaultCtx = context.WithValue(defaultCtx, reqWSKey{}, reqWS{name: "default"})
	for _, slug := range []string{"change-a", "change-b", "change-c"} {
		if _, err := s.CreateDocument(defaultCtx, slug, slug, "note", nil, nil, nil, "", "tester"); err != nil {
			t.Fatalf("create %s: %v", slug, err)
		}
	}

	alphaCtx, alphaRelease, err := s.Scoped(base, "alpha")
	if err != nil {
		t.Fatalf("scope alpha: %v", err)
	}
	if _, err := s.CreateDocument(alphaCtx, "alpha-change", "alpha", "note", nil, nil, nil, "", "tester"); err != nil {
		alphaRelease()
		t.Fatalf("create alpha doc: %v", err)
	}
	alphaRelease()

	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}
	page1, w := callListChanges(t, srv, defaultCtx, "/changes?limit=2")
	if w.Code != http.StatusOK || page1.Count != 2 {
		t.Fatalf("page 1: status=%d body=%s", w.Code, w.Body.String())
	}

	page2, w := callListChanges(t, srv, defaultCtx, "/changes?limit=2&since="+page1.NextCursor)
	if w.Code != http.StatusOK || page2.Count != 1 {
		t.Fatalf("page 2: status=%d body=%s", w.Code, w.Body.String())
	}

	empty, w := callListChanges(t, srv, defaultCtx, "/changes?since="+page2.NextCursor)
	if w.Code != http.StatusOK || empty.Count != 0 || empty.NextCursor != page2.NextCursor {
		t.Fatalf("empty page: status=%d body=%s", w.Code, w.Body.String())
	}

	alpha, w := callListChanges(t, srv, defaultCtx, "/changes?workspace=alpha")
	if w.Code != http.StatusOK || alpha.Count != 1 || alpha.Events[0].Slug == nil || *alpha.Events[0].Slug != "alpha-change" {
		t.Fatalf("alpha override: status=%d body=%s", w.Code, w.Body.String())
	}

	confined := context.WithValue(defaultCtx, reqWSKey{}, reqWS{name: "default", confined: true})
	_, w = callListChanges(t, srv, confined, "/changes?workspace=alpha")
	if w.Code != http.StatusForbidden {
		t.Fatalf("confined override status = %d, want %d (body %s)", w.Code, http.StatusForbidden, w.Body.String())
	}
}

func TestStreamChanges_ConfinedToken(t *testing.T) {
	s := testStore(t)
	srv := &Server{store: s, cfg: Config{APITokens: map[string]string{"tok123": "default"}}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /changes/stream", srv.auth(srv.streamChanges))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := ts.Client()
	req, err := http.NewRequest("GET", ts.URL+"/changes/stream?workspace=alpha", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer tok123")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 Forbidden for confined workspace override", resp.StatusCode)
	}
}

func TestStreamChanges_UnknownWorkspace(t *testing.T) {
	s := testStore(t)
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /changes/stream", srv.auth(srv.streamChanges))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := ts.Client()
	req, err := http.NewRequest("GET", ts.URL+"/changes/stream?workspace=nonexistentws", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 Not Found for unknown workspace", resp.StatusCode)
	}
}

func TestStreamChanges_BadCursor(t *testing.T) {
	s := testStore(t)
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /changes/stream", srv.auth(srv.streamChanges))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := ts.Client()
	req, err := http.NewRequest("GET", ts.URL+"/changes/stream?since=invalidcursor", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 Bad Request for bad cursor", resp.StatusCode)
	}
}

func TestChanges_CursorScopeMismatch_Workspace(t *testing.T) {
	s := testStore(t)
	base := context.Background()
	if _, err := s.CreateWorkspace(base, "alpha", "test workspace", "tester"); err != nil && !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}

	defaultCtx, release, err := s.Scoped(base, "default")
	if err != nil {
		t.Fatalf("scope default: %v", err)
	}
	defer release()
	defaultCtx = context.WithValue(defaultCtx, reqWSKey{}, reqWS{name: "default"})

	if _, err := s.CreateDocument(defaultCtx, "doc1", "Doc 1", "note", nil, nil, nil, "", "tester"); err != nil {
		t.Fatalf("create doc: %v", err)
	}
	page1, w := callListChanges(t, srv, defaultCtx, "/changes")
	if w.Code != http.StatusOK || page1.Count != 1 {
		t.Fatalf("page 1: status=%d body=%s", w.Code, w.Body.String())
	}

	// Carry cursor from default workspace to alpha workspace -> expect 400
	_, w = callListChanges(t, srv, defaultCtx, "/changes?workspace=alpha&since="+page1.NextCursor)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("cross workspace cursor status = %d, want 400 (body %s)", w.Code, w.Body.String())
	}
}

func TestChanges_CursorScopeMismatch_Kinds(t *testing.T) {
	s := testStore(t)
	base := context.Background()
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}

	defaultCtx, release, err := s.Scoped(base, "default")
	if err != nil {
		t.Fatalf("scope default: %v", err)
	}
	defer release()
	defaultCtx = context.WithValue(defaultCtx, reqWSKey{}, reqWS{name: "default"})

	if _, err := s.CreateDocument(defaultCtx, "doc2", "Doc 2", "note", nil, nil, nil, "", "tester"); err != nil {
		t.Fatalf("create doc: %v", err)
	}

	page1, w := callListChanges(t, srv, defaultCtx, "/changes?kind=doc_created")
	if w.Code != http.StatusOK || page1.Count != 1 {
		t.Fatalf("page 1: status=%d body=%s", w.Code, w.Body.String())
	}

	// Carry cursor from kind=doc_created to kind=task_completed -> expect 400
	_, w = callListChanges(t, srv, defaultCtx, "/changes?kind=task_completed&since="+page1.NextCursor)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("changed kind cursor status = %d, want 400 (body %s)", w.Code, w.Body.String())
	}
}

func TestStreamChanges_ConfinedToken_SameWorkspace(t *testing.T) {
	s := testStore(t)
	srv := &Server{store: s, cfg: Config{APITokens: map[string]string{"tok123": "default"}}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /changes/stream", srv.auth(srv.streamChanges))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := ts.Client()
	req, err := http.NewRequest("GET", ts.URL+"/changes/stream", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer tok123")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 OK for confined token own workspace", resp.StatusCode)
	}

	lineChan := make(chan string, 10)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			lineChan <- scanner.Text()
		}
	}()

	ctx, release, err := s.Scoped(context.Background(), "default")
	if err != nil {
		t.Fatalf("scope default: %v", err)
	}
	defer release()

	if _, err := s.CreateDocument(ctx, "confined-doc", "Confined Doc", "note", nil, nil, nil, "", "tester"); err != nil {
		t.Fatalf("create doc: %v", err)
	}

	var dataLine string
	timeout := time.After(3 * time.Second)
	for dataLine == "" {
		select {
		case line := <-lineChan:
			if strings.HasPrefix(line, "data: ") {
				dataLine = line
			}
		case <-timeout:
			t.Fatalf("timed out waiting for event on confined stream")
		}
	}

	var ev Event
	if err := json.Unmarshal([]byte(strings.TrimPrefix(dataLine, "data: ")), &ev); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if ev.Slug == nil || *ev.Slug != "confined-doc" {
		t.Errorf("slug = %v, want confined-doc", ev.Slug)
	}
}

func TestNormalizeKinds_EqualityAndColon(t *testing.T) {
	norm1 := normalizeKinds([]string{"a,b"})
	norm2 := normalizeKinds([]string{"a", "b"})
	hash1 := kindsHash(norm1)
	hash2 := kindsHash(norm2)
	if hash1 != hash2 {
		t.Fatalf("kindsHash([\"a,b\"]) = %s, kindsHash([\"a\",\"b\"]) = %s (want equal)", hash1, hash2)
	}

	normColon := normalizeKinds([]string{"doc:created", "task:completed"})
	hashColon := kindsHash(normColon)
	if len(hashColon) != 64 {
		t.Fatalf("hash length = %d, want 64", len(hashColon))
	}
}

func TestChanges_CursorScopeMismatch_EmbeddedNewline(t *testing.T) {
	s := testStore(t)
	base := context.Background()
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}

	defaultCtx, release, err := s.Scoped(base, "default")
	if err != nil {
		t.Fatalf("scope default: %v", err)
	}
	defer release()
	defaultCtx = context.WithValue(defaultCtx, reqWSKey{}, reqWS{name: "default"})

	page1, w := callListChanges(t, srv, defaultCtx, "/changes?kind=doc_created&kind=task_claimed%0Atask_completed")
	if w.Code != http.StatusOK {
		t.Fatalf("page 1: status=%d body=%s", w.Code, w.Body.String())
	}

	_, w = callListChanges(t, srv, defaultCtx, "/changes?kind=doc_created&kind=task_claimed&kind=task_completed&since="+page1.NextCursor)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("counterexample cursor scope mismatch status = %d, want 400 (body %s)", w.Code, w.Body.String())
	}
}

func TestRejectUnknownQueryParams(t *testing.T) {
	cases := []struct {
		name       string
		query      string
		allowed    []string
		wantReject bool
	}{
		{"empty query", "", []string{"since", "limit"}, false},
		{"only allowed", "?since=123&limit=10", []string{"since", "limit"}, false},
		{"one unknown", "?since=123&foo=bar", []string{"since", "limit"}, true},
		{"all unknown", "?foo=bar&baz=qux", []string{"since", "limit"}, true},
		{"case sensitive", "?Since=123", []string{"since", "limit"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/test"+tc.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			got := rejectUnknownQueryParams(w, req, tc.allowed...)
			if got != tc.wantReject {
				t.Fatalf("rejectUnknownQueryParams(...) = %v, want %v", got, tc.wantReject)
			}
			if got {
				if w.Code != http.StatusBadRequest {
					t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
				}
				var body map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
					t.Fatalf("failed to decode body: %v", err)
				}
				errObj, ok := body["error"].(map[string]any)
				if !ok {
					t.Fatalf("missing error object in response: %s", w.Body.String())
				}
				if code := errObj["code"]; code != "bad_request" {
					t.Errorf("error.code = %v, want bad_request", code)
				}
				msg, _ := errObj["message"].(string)
				if !strings.Contains(msg, "unrecognised query parameter") {
					t.Errorf("error.message = %q, want it to contain 'unrecognised query parameter'", msg)
				}
			}
		})
	}
}
