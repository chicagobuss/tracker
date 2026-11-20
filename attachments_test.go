package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A PNG header: 0x89 is invalid UTF-8 and there's a NUL, the two things
// Postgres text rejects outright.
var pngBytes = []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00")

func TestSearchText(t *testing.T) {
	for _, c := range []struct {
		ct, in, want string
	}{
		{"text/markdown", "hello", "hello"},
		{"text/plain; charset=utf-8", "a\x00b", "ab"},
		{"application/json", `{"a":1}`, `{"a":1}`},
		{"application/vnd.api+json", `{}`, `{}`},
		{"", "plain", "plain"},
		{"image/png", string(pngBytes), ""},
		{"image/svg+xml", "<svg/>", ""},
		{"application/pdf", "%PDF-1.7", ""},
		{"text/plain", "\x89bad", ""},
	} {
		if got := searchText(c.ct, []byte(c.in)); got != c.want {
			t.Errorf("searchText(%q, %q) = %q, want %q", c.ct, c.in, got, c.want)
		}
	}
}

func TestBinaryContentWrites(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	doc, err := s.CreateDocument(ctx, "pics/dot.png", "A red dot", "attachment", nil, nil, pngBytes, "image/png", "tester")
	if err != nil {
		t.Fatalf("create with binary content: %v", err)
	}
	lease, err := s.AcquireLease(ctx, doc.ID, "tester", "rewrite", time.Minute, "")
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	next := append([]byte{}, pngBytes...)
	next = append(next, 0xff, 0x00)
	if _, err := s.WriteContent(ctx, doc.ID, "tester", lease.LeaseToken, doc.Version, "image/png", next); err != nil {
		t.Fatalf("write binary content: %v", err)
	}
	if err := s.ReleaseLease(ctx, doc.ID, lease.LeaseToken); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Renaming re-indexes title + content; a binary doc must index the title alone.
	title := "A bigger red dot"
	if _, err := s.PatchDocument(ctx, doc.ID, DocPatch{Title: &title}, "tester"); err != nil {
		t.Fatalf("rename binary doc: %v", err)
	}
	docs, _, err := s.ListDocuments(ctx, "bigger", "", "", "", "", 10, 0)
	if err != nil || len(docs) != 1 || docs[0].ID != doc.ID {
		t.Fatalf("title search found %d docs (err %v), want the attachment", len(docs), err)
	}
}

func TestAttachmentHTTP(t *testing.T) {
	s := testStore(t)
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}
	if _, err := s.CreateDocument(context.Background(), "pics", "Pictures", "folio", nil, nil, nil, "", "tester"); err != nil {
		t.Fatalf("create folio: %v", err)
	}
	if _, err := s.CreateDocument(context.Background(), "pics/note.md", "note", "note", nil, nil, []byte("text"), "", "tester"); err != nil {
		t.Fatalf("create text doc: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /folios/{slug}/files", srv.auth(srv.createFolioFile))
	mux.HandleFunc("GET /docs/{rest...}", srv.auth(srv.getDoc))
	mux.HandleFunc("POST /mcp", srv.auth(srv.mcpHandler))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	post := func(path string, body any) map[string]any {
		t.Helper()
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", ts.URL+path, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Actor", "tester")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		if resp.StatusCode >= 300 {
			t.Fatalf("POST %s: %d %v", path, resp.StatusCode, out)
		}
		return out
	}

	// REST upload: type comes from the filename, the response carries an embed.
	out := post("/folios/pics/files", map[string]any{"filename": "red dot#2(1).png", "content_base64": base64.StdEncoding.EncodeToString(pngBytes)})
	doc := out["document"].(map[string]any)
	if doc["content_type"] != "image/png" {
		t.Errorf("content_type = %v, want image/png", doc["content_type"])
	}
	rawPath := "/docs/" + doc["id"].(string) + "/raw?workspace=default"
	if out["embed"] != "![red dot#2(1).png]("+rawPath+")" {
		t.Errorf("embed = %v", out["embed"])
	}

	// The raw route serves the exact bytes, typed and unsniffable.
	resp, err := http.Get(ts.URL + rawPath)
	if err != nil {
		t.Fatalf("raw: %v", err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, pngBytes) || resp.Header.Get("Content-Type") != "image/png" || resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("raw = %d bytes, type %q, nosniff %q", len(got), resp.Header.Get("Content-Type"), resp.Header.Get("X-Content-Type-Options"))
	}
	if resp.Header.Get("Content-Security-Policy") != "" {
		t.Errorf("an image should not be sandboxed: %q", resp.Header.Get("Content-Security-Policy"))
	}

	// Uploaded SVG could run script with this origin's access; it is sandboxed.
	svg := post("/folios/pics/files", map[string]any{"filename": "x.svg", "content_base64": base64.StdEncoding.EncodeToString([]byte(`<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>`))})
	if embed, _ := svg["embed"].(string); !strings.HasPrefix(embed, "![x.svg](") {
		t.Fatalf("missing SVG embed: %v", svg)
	}
	resp, err = http.Get(ts.URL + "/docs/pics/x.svg/raw")
	if err != nil {
		t.Fatalf("raw svg: %v", err)
	}
	resp.Body.Close()
	if resp.Header.Get("Content-Security-Policy") != "sandbox" {
		t.Errorf("svg CSP = %q, want sandbox", resp.Header.Get("Content-Security-Policy"))
	}

	// Legacy text clients send content:"" when creating an unseeded document.
	stub := post("/folios/pics/files", map[string]any{"filename": "stub.md", "content": ""})
	if d := stub["document"].(map[string]any); d["version"] != float64(0) {
		t.Fatalf("empty text seeded a version: %v", stub)
	}

	// An explicitly empty base64 file still has downloadable content.
	empty := post("/folios/pics/files", map[string]any{"filename": "empty.zip", "content_base64": ""})
	file := empty["document"].(map[string]any)
	if file["version"] != float64(1) || file["size_bytes"] != float64(0) || empty["embed"] == nil {
		t.Fatalf("zero-byte upload: %v", empty)
	}
	resp, err = http.Get(ts.URL + "/docs/" + file["id"].(string) + "/raw")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || len(body) != 0 {
		t.Fatalf("empty download: status=%d bytes=%d err=%v", resp.StatusCode, len(body), err)
	}

	// MCP: upload by base64, read back without flooding the caller.
	call := func(name string, args map[string]any) map[string]any {
		t.Helper()
		out := post("/mcp", map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call",
			"params": map[string]any{"name": name, "arguments": args}})
		res := out["result"].(map[string]any)
		text := res["content"].([]any)[0].(map[string]any)["text"].(string)
		if res["isError"] == true {
			t.Fatalf("%s: %s", name, text)
		}
		var v map[string]any
		_ = json.Unmarshal([]byte(text), &v)
		return v
	}
	up := call("add_folio_file", map[string]any{"slug": "pics", "filename": "dot2.png", "content_base64": base64.StdEncoding.EncodeToString(pngBytes)})
	if !strings.HasPrefix(up["embed"].(string), "![dot2.png](") {
		t.Errorf("add_folio_file embed = %v", up["embed"])
	}
	got2 := call("get_doc", map[string]any{"id": "pics/dot2.png", "include_content": true})
	if _, has := got2["content"]; has || got2["content_omitted"] == nil {
		t.Errorf("get_doc inlined a binary doc: %v", got2)
	}
	raw := call("get_raw", map[string]any{"id": "pics/dot2.png"})
	if b, _ := base64.StdEncoding.DecodeString(raw["content_base64"].(string)); !bytes.Equal(b, pngBytes) {
		t.Errorf("get_raw round-trip differs")
	}
	if txt := call("get_doc", map[string]any{"id": "pics/note.md", "include_content": true}); txt["content"] != "text" || txt["embed"] != nil {
		t.Errorf("text doc changed shape: %v", txt)
	}
}

func TestUpdateDocRequiresExplicitContent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	srv := &Server{store: s}
	doc, err := s.CreateDocument(ctx, "report", "Report", "attachment", nil, nil, pngBytes, "image/png", "tester")
	if err != nil {
		t.Fatal(err)
	}
	update := mcpTools["update_doc"].fn
	for _, args := range []targs{
		{"id": doc.ID},
		{"id": doc.ID, "content": nil},
		{"id": doc.ID, "content_base64": 4},
		{"id": doc.ID, "content": "", "content_base64": ""},
	} {
		if _, err := update(ctx, srv, "tester", args); err == nil {
			t.Fatalf("update accepted %v", args)
		}
	}
	if _, err := update(ctx, srv, "tester", targs{"id": doc.ID, "content_base64": "", "content_type": "application/pdf"}); err != nil {
		t.Fatal(err)
	}
	history, err := s.DocRevisions(ctx, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[0].ContentType != "application/pdf" || history[0].SizeBytes != 0 ||
		history[1].ContentType != "image/png" || history[1].SizeBytes != int64(len(pngBytes)) {
		t.Fatalf("unexpected revision history: %+v", history)
	}
	key, ct, err := s.RevisionContent(ctx, doc.ID, 1)
	if err != nil || ct != "image/png" || key != doc.ContentKey {
		t.Fatalf("original revision: key=%s type=%s err=%v", key, ct, err)
	}
}

func TestRawAttachmentOverInlineLimit(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	srv := &Server{store: s}
	doc, err := s.CreateDocument(ctx, "large.bin", "large", "attachment", nil, nil,
		make([]byte, maxInlineContent+1), "application/octet-stream", "tester")
	if err != nil {
		t.Fatal(err)
	}
	out, err := mcpTools["get_raw"].fn(ctx, srv, "", targs{"id": doc.ID})
	if err == nil || !strings.Contains(err.Error(), "content_url") || out != nil {
		t.Fatalf("expected a download instruction without partial data: err=%v", err)
	}
	envelope, err := srv.docWithExtras(ctx, doc, true)
	if err != nil || envelope["content_url"] == nil || envelope["content_omitted"] == nil {
		t.Fatalf("missing download location: %v, %v", envelope, err)
	}
}
