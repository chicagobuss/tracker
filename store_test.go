package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Every backend must hand out a content_url rooted at BASE_URL. S3_ENDPOINT is
// tracker's own route to the bucket — typically loopback or a compose service
// name — and SigV4 binds the host into the signature, so presigning against it
// produced URLs that no other machine could fetch. Needs no database: the S3
// backend's client is never touched on this path, which is the point.
func TestPresignGetObject_RootedAtBaseURL(t *testing.T) {
	const baseURL = "http://10.0.0.5:8770"
	const blobKey = "sha256/deadbeef"
	key := []byte("0123456789abcdef0123456789abcdef")

	backends := map[string]BlobStore{
		"file": &LocalBlobStore{blobDir: t.TempDir(), baseURL: baseURL, signingKey: key},
		"s3":   &S3BlobStore{bucket: "blobs", baseURL: baseURL, signingKey: key},
	}
	for name, bs := range backends {
		t.Run(name, func(t *testing.T) {
			got, err := bs.PresignGetObject(context.Background(), blobKey, 15*time.Minute)
			if err != nil {
				t.Fatalf("presign: %v", err)
			}
			if want := baseURL + "/blobs/" + blobKey + "?"; !strings.HasPrefix(got, want) {
				t.Fatalf("content_url = %q, want prefix %q", got, want)
			}
			// And the signature has to verify, or the URL 401s once auth is on.
			if !blobSigOK(Config{BlobSigningKey: key}, httptest.NewRequest("GET", got, nil)) {
				t.Errorf("signature on %q did not verify", got)
			}
		})
	}
}

func TestAcquireLease_FreshDoc(t *testing.T) {
	s := testStore(t)
	doc := newDoc(t, s, "fresh")

	l, err := s.AcquireLease(context.Background(), doc.ID, "agent-a", "writing", time.Minute, "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if l.LeaseToken == "" {
		t.Error("expected a lease token, got empty string")
	}
	if l.Owner != "agent-a" {
		t.Errorf("owner = %q, want agent-a", l.Owner)
	}
	if !l.ExpiresAt.After(time.Now()) {
		t.Errorf("expires_at = %v, want a future time", l.ExpiresAt)
	}
}

func TestCreateDocument_DuplicateSlug(t *testing.T) {
	s := testStore(t)
	newDoc(t, s, "unique")

	_, err := s.CreateDocument(context.Background(), "unique", "Duplicate", "note",
		nil, nil, nil, "text/markdown", "tester")
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("err = %v, want ErrAlreadyExists", err)
	}
}

func TestAcquireLease_DeniedWhileHeld(t *testing.T) {
	s := testStore(t)
	doc := newDoc(t, s, "contested")
	leaseFor(t, s, doc.ID, "agent-a", time.Minute)

	// A second agent must be denied, and must learn who holds it.
	held, err := s.AcquireLease(context.Background(), doc.ID, "agent-b", "also writing", time.Minute, "")
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("err = %v, want ErrLeaseHeld", err)
	}
	if held == nil || held.Owner != "agent-a" {
		t.Errorf("denied lease should report the live holder (agent-a), got %+v", held)
	}
}

func TestAcquireLease_RenewWithMatchingToken(t *testing.T) {
	s := testStore(t)
	doc := newDoc(t, s, "renewable")
	tok := leaseFor(t, s, doc.ID, "agent-a", time.Second)

	// Same owner presenting the live token renews in place — same token, later expiry.
	renewed, err := s.AcquireLease(context.Background(), doc.ID, "agent-a", "still writing", time.Hour, tok)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if renewed.LeaseToken != tok {
		t.Errorf("renew minted a new token %q, want the existing %q", renewed.LeaseToken, tok)
	}
	if !renewed.ExpiresAt.After(time.Now().Add(30 * time.Minute)) {
		t.Errorf("expires_at = %v, want ~1h out (renew should extend the TTL)", renewed.ExpiresAt)
	}
}

// A crashed agent must not be able to block a doc forever: once its lease
// expires, another agent steals it and gets a fresh token.
func TestAcquireLease_StealExpired(t *testing.T) {
	s := testStore(t)
	doc := newDoc(t, s, "abandoned")

	// A negative TTL yields an already-expired lease — a crashed holder, without
	// making the test sleep.
	dead, err := s.AcquireLease(context.Background(), doc.ID, "agent-a", "crashed", -time.Second, "")
	if err != nil {
		t.Fatalf("seed expired lease: %v", err)
	}

	stolen, err := s.AcquireLease(context.Background(), doc.ID, "agent-b", "taking over", time.Minute, "")
	if err != nil {
		t.Fatalf("steal expired lease: %v", err)
	}
	if stolen.Owner != "agent-b" {
		t.Errorf("owner = %q, want agent-b", stolen.Owner)
	}
	if stolen.LeaseToken == dead.LeaseToken {
		t.Error("steal reused the dead lease's token; it must mint a fresh one")
	}
}

func TestAcquireLease_ConcurrentSingleWinner(t *testing.T) {
	s := testStore(t)
	doc := newDoc(t, s, "thundering-herd")

	const agents = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	var won, denied int

	for i := 0; i < agents; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.AcquireLease(context.Background(), doc.ID, "agent", "race", time.Minute, "")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won++
			case errors.Is(err, ErrLeaseHeld):
				denied++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// The row lock in AcquireLease must serialize these: exactly one winner.
	if won != 1 {
		t.Errorf("%d agents acquired the lease, want exactly 1 (denied=%d)", won, denied)
	}
}

func TestAcquireLease_MissingDoc(t *testing.T) {
	s := testStore(t)
	_, err := s.AcquireLease(context.Background(), "00000000-0000-0000-0000-000000000000", "agent-a", "", time.Minute, "")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestReleaseLease_RequiresMatchingToken(t *testing.T) {
	s := testStore(t)
	doc := newDoc(t, s, "releasable")
	tok := leaseFor(t, s, doc.ID, "agent-a", time.Minute)

	if err := s.ReleaseLease(context.Background(), doc.ID, "not-the-token"); !errors.Is(err, ErrNoLease) {
		t.Fatalf("release with wrong token: err = %v, want ErrNoLease", err)
	}
	if err := s.ReleaseLease(context.Background(), doc.ID, tok); err != nil {
		t.Fatalf("release with correct token: %v", err)
	}
	// Released, so the next agent acquires cleanly.
	if _, err := s.AcquireLease(context.Background(), doc.ID, "agent-b", "", time.Minute, ""); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
}

func TestWriteContent_Success(t *testing.T) {
	s := testStore(t)
	doc := newDoc(t, s, "writable")
	tok := leaseFor(t, s, doc.ID, "agent-a", time.Minute)

	body := []byte("# hello\n\nfirst revision\n")
	got, err := s.WriteContent(context.Background(), doc.ID, "agent-a", tok, doc.Version, "text/markdown", body)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if got.Version != doc.Version+1 {
		t.Errorf("version = %d, want %d", got.Version, doc.Version+1)
	}
	if got.UpdatedBy != "agent-a" {
		t.Errorf("updated_by = %q, want agent-a", got.UpdatedBy)
	}
	if b := readContent(t, s, got.ContentKey); string(b) != string(body) {
		t.Errorf("stored content = %q, want %q", b, body)
	}

	// The write must be recorded in the revision history.
	revs, err := s.DocRevisions(context.Background(), doc.ID)
	if err != nil {
		t.Fatalf("revisions: %v", err)
	}
	if len(revs) != 1 {
		t.Fatalf("got %d revisions, want 1", len(revs))
	}
	if revs[0].Author != "agent-a" {
		t.Errorf("revision author = %q, want agent-a", revs[0].Author)
	}
}

// The two-layer write guard: no lease, wrong token, wrong owner, or an expired
// lease must all be rejected — these are the cases that would let one agent
// clobber another's work.
func TestWriteContent_LeaseGuard(t *testing.T) {
	ctx := context.Background()

	t.Run("no lease at all", func(t *testing.T) {
		s := testStore(t)
		doc := newDoc(t, s, "no-lease")
		_, err := s.WriteContent(ctx, doc.ID, "agent-a", "some-token", doc.Version, "text/markdown", []byte("x"))
		if !errors.Is(err, ErrNoLease) {
			t.Fatalf("err = %v, want ErrNoLease", err)
		}
	})

	t.Run("wrong token", func(t *testing.T) {
		s := testStore(t)
		doc := newDoc(t, s, "wrong-token")
		leaseFor(t, s, doc.ID, "agent-a", time.Minute)
		_, err := s.WriteContent(ctx, doc.ID, "agent-a", "bogus-token", doc.Version, "text/markdown", []byte("x"))
		if !errors.Is(err, ErrNoLease) {
			t.Fatalf("err = %v, want ErrNoLease", err)
		}
	})

	t.Run("right token, wrong actor", func(t *testing.T) {
		s := testStore(t)
		doc := newDoc(t, s, "wrong-actor")
		tok := leaseFor(t, s, doc.ID, "agent-a", time.Minute)
		// agent-b somehow has agent-a's token: the owner check must still reject it.
		_, err := s.WriteContent(ctx, doc.ID, "agent-b", tok, doc.Version, "text/markdown", []byte("x"))
		if !errors.Is(err, ErrNoLease) {
			t.Fatalf("err = %v, want ErrNoLease", err)
		}
	})

	t.Run("expired lease", func(t *testing.T) {
		s := testStore(t)
		doc := newDoc(t, s, "expired")
		tok := leaseFor(t, s, doc.ID, "agent-a", -time.Second) // already dead
		_, err := s.WriteContent(ctx, doc.ID, "agent-a", tok, doc.Version, "text/markdown", []byte("x"))
		if !errors.Is(err, ErrNoLease) {
			t.Fatalf("err = %v, want ErrNoLease", err)
		}
	})
}

// Optimistic concurrency: holding the lease is not enough — the write must also
// be based on the version the caller last read.
func TestWriteContent_VersionConflict(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	doc := newDoc(t, s, "cas")
	tok := leaseFor(t, s, doc.ID, "agent-a", time.Minute)

	stale := doc.Version
	if _, err := s.WriteContent(ctx, doc.ID, "agent-a", tok, stale, "text/markdown", []byte("v1")); err != nil {
		t.Fatalf("first write: %v", err)
	}

	// Same lease, but re-using the now-stale base version must be refused.
	_, err := s.WriteContent(ctx, doc.ID, "agent-a", tok, stale, "text/markdown", []byte("v2"))
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}
}

func TestWriteContent_SoftDeletedDoc(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	doc := newDoc(t, s, "deleted")
	tok := leaseFor(t, s, doc.ID, "agent-a", time.Minute)

	if _, err := s.SoftDeleteDocument(ctx, doc.ID, "agent-a", false); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	_, err := s.WriteContent(ctx, doc.ID, "agent-a", tok, doc.Version, "text/markdown", []byte("x"))
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("err = %v, want ErrDeleted", err)
	}
}

// Same bytes written twice must land on one content-addressed blob.
func TestWriteContent_BlobDedupe(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	body := []byte("identical bytes")

	a := newDoc(t, s, "dedupe-a")
	tokA := leaseFor(t, s, a.ID, "agent-a", time.Minute)
	wa, err := s.WriteContent(ctx, a.ID, "agent-a", tokA, a.Version, "text/markdown", body)
	if err != nil {
		t.Fatalf("write a: %v", err)
	}

	b := newDoc(t, s, "dedupe-b")
	tokB := leaseFor(t, s, b.ID, "agent-b", time.Minute)
	wb, err := s.WriteContent(ctx, b.ID, "agent-b", tokB, b.Version, "text/markdown", body)
	if err != nil {
		t.Fatalf("write b: %v", err)
	}

	if wa.ContentKey != wb.ContentKey {
		t.Errorf("content keys differ (%q vs %q); identical bytes must dedupe to one blob", wa.ContentKey, wb.ContentKey)
	}
}

// validWorkspace is a security boundary, not cosmetics: it is the only thing
// standing between a caller-supplied X-Workspace and set_config, so the
// AllWorkspaces sentinel — which lifts RLS entirely — must be unreachable
// through it. Needs no database.
func TestValidWorkspace(t *testing.T) {
	valid := []string{"default", "alpha", "greenfield-1", "a", "a_b-c", "x0123456789"}
	for _, name := range valid {
		if !validWorkspace(name) {
			t.Errorf("validWorkspace(%q) = false, want true", name)
		}
	}
	invalid := []string{
		AllWorkspaces, "", "Alpha", "-leading", "_leading", "has space",
		"has.dot", "a'; drop table documents; --", "ünicode",
		"tooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong",
	}
	for _, name := range invalid {
		if validWorkspace(name) {
			t.Errorf("validWorkspace(%q) = true, want false", name)
		}
	}
}

// The isolation guarantee itself. These calls go through the ordinary Store
// methods, none of which filter by workspace in their SQL — if this passes, the
// RLS policies are what is doing the work.
func TestWorkspaceIsolation(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// testStore truncates documents but deliberately not workspaces, so on a
	// re-run this workspace is already registered.
	if _, err := s.CreateWorkspace(ctx, "alpha", "greenfield", "tester"); err != nil &&
		!errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("create workspace: %v", err)
	}

	defCtx, defRelease, err := s.Scoped(ctx, "default")
	if err != nil {
		t.Fatalf("scope default: %v", err)
	}
	defer defRelease()
	altCtx, altRelease, err := s.Scoped(ctx, "alpha")
	if err != nil {
		t.Fatalf("scope alpha: %v", err)
	}
	defer altRelease()

	if _, err := s.CreateDocument(defCtx, "shared", "In default", "note", nil, nil, nil, "text/markdown", "tester"); err != nil {
		t.Fatalf("create in default: %v", err)
	}
	// The same slug must be free in another workspace — globally-unique slugs
	// would make throwaway experiments collide with real work.
	if _, err := s.CreateDocument(altCtx, "shared", "In alpha", "note", nil, nil, nil, "text/markdown", "tester"); err != nil {
		t.Fatalf("same slug in another workspace: %v", err)
	}

	for _, tc := range []struct {
		name, wantTitle string
		ctx             context.Context
	}{
		{"default", "In default", defCtx},
		{"alpha", "In alpha", altCtx},
	} {
		docs, total, err := s.ListDocuments(tc.ctx, "", "", "", "", "exclude", 50, 0)
		if err != nil {
			t.Fatalf("list in %s: %v", tc.name, err)
		}
		if total != 1 {
			t.Errorf("%s sees %d documents, want only its own 1", tc.name, total)
		}
		if len(docs) == 1 && docs[0].Title != tc.wantTitle {
			t.Errorf("%s resolved slug 'shared' to %q, want %q", tc.name, docs[0].Title, tc.wantTitle)
		}
		// Full-text search must be scoped too — it is the whole reason for this.
		if _, hits, err := s.ListDocuments(tc.ctx, "default alpha", "", "", "", "exclude", 50, 0); err != nil {
			t.Fatalf("search in %s: %v", tc.name, err)
		} else if hits > 1 {
			t.Errorf("search in %s returned %d hits, want at most its own 1", tc.name, hits)
		}
	}

	// A document created in one workspace must not be reachable by id from the
	// other, even though the caller knows the id.
	alphaDoc, err := s.GetDocument(altCtx, "shared")
	if err != nil {
		t.Fatalf("get in alpha: %v", err)
	}
	if _, err := s.GetDocument(defCtx, alphaDoc.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("default fetched alpha's document by id: err = %v, want ErrNotFound", err)
	}

	// Tasks partition on the same boundary.
	if _, err := s.CreateTask(altCtx, "alpha work", nil, "tester"); err != nil {
		t.Fatalf("create task in alpha: %v", err)
	}
	if tasks, total, err := s.ListTasks(defCtx, "", 50, 0); err != nil {
		t.Fatalf("list tasks in default: %v", err)
	} else if len(tasks) != 0 || total != 0 {
		t.Errorf("default sees %d of alpha's tasks (total %d), want 0", len(tasks), total)
	}
}

// An unregistered workspace must be reported, not silently treated as a new
// empty one — otherwise a typo'd X-Workspace looks like a wiped store.
func TestScopedRejectsUnknownWorkspace(t *testing.T) {
	s := testStore(t)
	if _, _, err := s.Scoped(context.Background(), "no-such-workspace"); !errors.Is(err, ErrNoWorkspace) {
		t.Fatalf("err = %v, want ErrNoWorkspace", err)
	}
}

// The per-call workspace override is a convenience for working across
// workspaces from one session, so its refusals are the interesting part: it
// must not become a way around a confined token.
func TestScopeOverride(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if _, err := s.CreateWorkspace(ctx, "alpha", "greenfield", "tester"); err != nil &&
		!errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("create workspace: %v", err)
	}
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}

	// A request pinned to "default" by an unconfined token.
	free := context.WithValue(ctx, reqWSKey{}, reqWS{name: "default"})
	confined := context.WithValue(ctx, reqWSKey{}, reqWS{name: "alpha", confined: true})

	t.Run("allowed for a read on an unconfined token", func(t *testing.T) {
		scoped, release, err := srv.scopeOverride(free, "alpha")
		if err != nil {
			t.Fatalf("override: %v", err)
		}
		defer release()
		if _, total, err := s.ListDocuments(scoped, "", "", "", "", "exclude", 50, 0); err != nil {
			t.Fatalf("list: %v", err)
		} else if total != 0 {
			// testStore truncated documents, so alpha is empty — the point is
			// that the call succeeded against alpha rather than being refused.
			t.Logf("alpha holds %d documents", total)
		}
	})

	t.Run("allowed for a write on an unconfined token", func(t *testing.T) {
		scoped, release, err := srv.scopeOverride(free, "alpha")
		if err != nil {
			t.Fatalf("override: %v", err)
		}
		defer release()
		if _, err := s.CreateDocument(scoped, "override-write", "t", "note", nil, nil, nil, "", "tester"); err != nil {
			t.Fatalf("create: %v", err)
		}
		// Readable from the override target...
		if _, err := s.GetDocument(scoped, "override-write"); err != nil {
			t.Errorf("alpha cannot read the doc it just wrote: %v", err)
		}
		// ...and invisible from the connection's own workspace.
		def, defRelease, err := s.Scoped(ctx, "default")
		if err != nil {
			t.Fatalf("scope default: %v", err)
		}
		defer defRelease()
		if _, err := s.GetDocument(def, "override-write"); !errors.Is(err, ErrNotFound) {
			t.Errorf("default workspace sees the doc: err = %v, want not found", err)
		}
	})

	for _, tc := range []struct {
		name, ws, wantSubstr string
		ctx                  context.Context
	}{
		{"confined token cannot escape", "default", "confined to workspace", confined},
		{"sentinel is unreachable", AllWorkspaces, "invalid", free},
		{"invalid name refused", "Alpha", "invalid", free},
		{"unknown workspace refused", "no-such-ws", "unknown workspace", free},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, release, err := srv.scopeOverride(tc.ctx, tc.ws)
			if err == nil {
				release()
				t.Fatalf("override to %q was allowed; want refusal", tc.ws)
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("err = %q, want it to mention %q", err, tc.wantSubstr)
			}
		})
	}
}

// Every workspace-scoped tool should advertise the override; the two registry
// tools (list_workspaces, create_workspace) should not.
func TestWorkspaceArgOnScopedTools(t *testing.T) {
	for _, d := range mcpToolDescriptors() {
		name := d["name"].(string)
		schema := d["inputSchema"].(map[string]any)
		props, _ := schema["properties"].(map[string]any)
		_, has := props["workspace"]

		want := name != "list_workspaces" && name != "create_workspace"
		if has != want {
			t.Errorf("tool %q: workspace arg present = %v, want %v", name, has, want)
		}
	}
}

// The override is decided in mcpHandler, so pin its edge cases end to end:
// registry tools must ignore a workspace passed by a client the schema never
// offered one to, and a confined token naming its own workspace must sail
// through the bypass rather than trip the confined check.
func TestMCPHandlerWorkspaceOverride(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if _, err := s.CreateWorkspace(ctx, "alpha", "greenfield", "tester"); err != nil &&
		!errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("create workspace: %v", err)
	}
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}

	pinned := func(name string, confined bool) (context.Context, func()) {
		scoped, release, err := s.Scoped(ctx, name)
		if err != nil {
			t.Fatalf("scope %q: %v", name, err)
		}
		return context.WithValue(scoped, reqWSKey{}, reqWS{name: name, confined: confined}), release
	}

	call := func(ctx context.Context, name string, args map[string]any) (bool, string) {
		params, _ := json.Marshal(map[string]any{"name": name, "arguments": args})
		raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1,
			"method": "tools/call", "params": json.RawMessage(params)})
		req := httptest.NewRequest("POST", "/mcp", bytes.NewReader(raw)).WithContext(ctx)
		req.Header.Set("X-Actor", "tester")
		w := httptest.NewRecorder()
		srv.mcpHandler(w, req)
		return strings.Contains(w.Body.String(), `"isError":true`), w.Body.String()
	}

	t.Run("registry tool ignores a passed workspace", func(t *testing.T) {
		c, release := pinned("default", false)
		defer release()
		// no-such-ws would fail scopeOverride; success proves it was ignored.
		if isErr, body := call(c, "list_workspaces", map[string]any{"workspace": "no-such-ws"}); isErr {
			t.Fatalf("list_workspaces honoured the override: %s", body)
		}
	})

	t.Run("confined token naming its own workspace succeeds", func(t *testing.T) {
		c, release := pinned("alpha", true)
		defer release()
		if isErr, body := call(c, "list_docs", map[string]any{"workspace": "alpha"}); isErr {
			t.Fatalf("own-workspace call refused: %s", body)
		}
	})

	t.Run("write through the handler lands in the override workspace", func(t *testing.T) {
		c, release := pinned("default", false)
		defer release()
		if isErr, body := call(c, "create_doc", map[string]any{"slug": "handler-write", "workspace": "alpha"}); isErr {
			t.Fatalf("create_doc refused: %s", body)
		}
		def, defRelease, err := s.Scoped(ctx, "default")
		if err != nil {
			t.Fatalf("scope default: %v", err)
		}
		defer defRelease()
		if _, err := s.GetDocument(def, "handler-write"); !errors.Is(err, ErrNotFound) {
			t.Errorf("default workspace sees the doc: err = %v, want not found", err)
		}
	})
}

// The regression this fixes: a folio file created with tags was not findable by
// tag. add_folio_file took no tags argument, so callers passed them as metadata
// — which the tag filter does not search — and the create silently succeeded
// with only the membership tag. Drive the real tool with JSON-shaped args, then
// query the way a caller looking for that document would.
func TestAddFolioFile_TagsAreSearchable(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}

	if _, err := mcpTools["create_folio"].fn(ctx, srv, "tester", targs{
		"slug": "beta-course", "title": "Beta",
	}); err != nil {
		t.Fatalf("create_folio: %v", err)
	}

	// []any, not []string: this is the shape a decoded JSON-RPC array arrives in,
	// and targs.strs only unwraps that one.
	if _, err := mcpTools["add_folio_file"].fn(ctx, srv, "tester", targs{
		"slug": "beta-course", "filename": "current.md", "kind": "decision",
		"tags": []any{"course:beta", "status:active"}, "content": "# Beta",
	}); err != nil {
		t.Fatalf("add_folio_file: %v", err)
	}

	for _, tag := range []string{"course:beta", "status:active", "folio:beta-course"} {
		docs, total, err := s.ListDocuments(ctx, "", "", tag, "", "exclude", 50, 0)
		if err != nil {
			t.Fatalf("list by %s: %v", tag, err)
		}
		if total != 1 {
			t.Fatalf("tag %s matched %d documents, want 1", tag, total)
		}
		if docs[0].Slug != "beta-course/current.md" {
			t.Errorf("tag %s matched %q, want beta-course/current.md", tag, docs[0].Slug)
		}
	}
}

// Membership is a tag, so a caller passing tags must not be able to drop the
// document out of its own folio.
func TestAddFolioFile_CallerTagsCannotDisplaceMembership(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}

	if _, err := mcpTools["create_folio"].fn(ctx, srv, "tester", targs{"slug": "f", "title": "F"}); err != nil {
		t.Fatalf("create_folio: %v", err)
	}
	if _, err := mcpTools["add_folio_file"].fn(ctx, srv, "tester", targs{
		"slug": "f", "filename": "a.md", "tags": []any{"topic:x"},
	}); err != nil {
		t.Fatalf("add_folio_file: %v", err)
	}

	// Query membership the way the folio itself does, rather than through the
	// MCP response envelope: get_folio summarises its files, so asserting on
	// that shape would test the envelope instead of the tag.
	docs, total, err := s.ListDocuments(ctx, "", "", folioTag("f"), "", "exclude", 50, 0)
	if err != nil {
		t.Fatalf("list by folio tag: %v", err)
	}
	if total != 1 {
		t.Fatalf("folio holds %d files, want 1 — caller tags displaced membership", total)
	}
	if docs[0].Slug != "f/a.md" {
		t.Errorf("folio member = %q, want f/a.md", docs[0].Slug)
	}
}

func TestEvents_EmitOnMutation(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// 1. Create document -> doc_created
	doc, err := s.CreateDocument(ctx, "evt-doc", "Event Test Doc", "note", []string{"tag1"}, nil, []byte("v1 content"), "text/markdown", "agent-1")
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	evs, _, err := s.ListEvents(ctx, "", nil, 50)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(evs) != 1 || evs[0].Kind != "doc_created" {
		t.Fatalf("expected 1 doc_created event, got %v", evs)
	}
	if evs[0].Slug == nil || *evs[0].Slug != "evt-doc" {
		t.Errorf("slug = %v, want evt-doc", evs[0].Slug)
	}

	// 2. Write content -> doc_updated
	lease, err := s.AcquireLease(ctx, doc.ID, "agent-1", "updating", time.Minute, "")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	_, err = s.WriteContent(ctx, doc.ID, "agent-1", lease.LeaseToken, 1, "text/markdown", []byte("v2 content"))
	if err != nil {
		t.Fatalf("WriteContent: %v", err)
	}

	// 3. Patch document -> doc_retagged
	title := "Renamed Title"
	_, err = s.PatchDocument(ctx, doc.ID, DocPatch{Title: &title, AddTags: []string{"tag2"}}, "agent-1")
	if err != nil {
		t.Fatalf("PatchDocument: %v", err)
	}

	// 4. Soft delete -> doc_soft_deleted
	_, err = s.SoftDeleteDocument(ctx, doc.ID, "agent-1", false)
	if err != nil {
		t.Fatalf("SoftDeleteDocument: %v", err)
	}

	// 5. Restore document -> doc_restored
	_, err = s.RestoreDocument(ctx, doc.ID, "agent-1")
	if err != nil {
		t.Fatalf("RestoreDocument: %v", err)
	}

	// 6. Hard delete -> doc_hard_deleted
	_, err = s.HardDeleteDocument(ctx, doc.ID, "evt-doc", "agent-1", false)
	if err != nil {
		t.Fatalf("HardDeleteDocument: %v", err)
	}

	// 7. Task operations -> task_enqueued, task_claimed, task_completed
	task, err := s.CreateTask(ctx, "Test Task", nil, "agent-1")
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	task, err = s.ClaimTask(ctx, task.ID, "agent-1", time.Minute)
	if err != nil {
		t.Fatalf("ClaimTask: %v", err)
	}
	_, err = s.CompleteTask(ctx, task.ID, "done", nil, "agent-1")
	if err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}

	// Retrieve all events
	allEvs, _, err := s.ListEvents(ctx, "", nil, 100)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	wantKinds := []string{
		"doc_created", "doc_updated", "doc_retagged", "doc_soft_deleted",
		"doc_restored", "doc_hard_deleted", "task_enqueued", "task_claimed", "task_completed",
	}
	if len(allEvs) != len(wantKinds) {
		t.Fatalf("got %d events, want %d", len(allEvs), len(wantKinds))
	}
	for i, k := range wantKinds {
		if allEvs[i].Kind != k {
			t.Errorf("event[%d].Kind = %q, want %q", i, allEvs[i].Kind, k)
		}
	}

	// Test cursor resumption without gaps or repeats
	page1, c1, err := s.ListEvents(ctx, "", nil, 4)
	if err != nil {
		t.Fatalf("ListEvents page 1: %v", err)
	}
	if len(page1) != 4 {
		t.Fatalf("page 1 count = %d, want 4", len(page1))
	}
	page2, _, err := s.ListEvents(ctx, c1, nil, 100)
	if err != nil {
		t.Fatalf("ListEvents page 2: %v", err)
	}
	if len(page2) != 5 {
		t.Fatalf("page 2 count = %d, want 5", len(page2))
	}
}

func TestEvents_CreateWithoutContent(t *testing.T) {
	s := testStore(t)
	doc, err := s.CreateDocument(context.Background(), "empty-event-doc", "Empty", "note", nil, nil, nil, "", "agent-1")
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}
	events, _, err := s.ListEvents(context.Background(), "", nil, 10)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events) != 1 || events[0].Kind != "doc_created" || events[0].DocID == nil || *events[0].DocID != doc.ID {
		t.Fatalf("events = %+v, want one doc_created for %s", events, doc.ID)
	}
}

func TestStreamChanges_FlushedAndResumes(t *testing.T) {
	s := testStore(t)
	ctx, release, err := s.Scoped(context.Background(), "default")
	if err != nil {
		t.Fatalf("Scoped: %v", err)
	}
	defer release()
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /changes/stream", srv.auth(srv.streamChanges))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := ts.Client()

	// 1. Connect to stream
	req, err := http.NewRequest("GET", ts.URL+"/changes/stream", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if contentType := resp.Header.Get("Content-Type"); contentType != "text/event-stream" {
		t.Fatalf("content-type = %q, want text/event-stream", contentType)
	}

	lineChan := make(chan string, 100)
	errChan := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			lineChan <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			errChan <- err
		}
	}()

	// Mutate document
	doc, err := s.CreateDocument(ctx, "stream-doc-1", "Stream Doc 1", "note", nil, nil, []byte("content 1"), "text/markdown", "tester")
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	// Assert event arrives promptly with SSE id: line
	var idLine, dataLine string
	timeout := time.After(3 * time.Second)
	for dataLine == "" {
		select {
		case line := <-lineChan:
			if strings.HasPrefix(line, "id: ") {
				idLine = strings.TrimPrefix(line, "id: ")
			} else if strings.HasPrefix(line, "data: ") {
				dataLine = line
			}
		case err := <-errChan:
			t.Fatalf("scanner error: %v", err)
		case <-timeout:
			t.Fatalf("timed out waiting for SSE data line (event frame was not flushed promptly)")
		}
	}

	if idLine == "" {
		t.Fatalf("expected SSE id: line, got empty")
	}

	var ev Event
	if err := json.Unmarshal([]byte(strings.TrimPrefix(dataLine, "data: ")), &ev); err != nil {
		t.Fatalf("unmarshal event json: %v", err)
	}
	if ev.Kind != "doc_created" {
		t.Errorf("kind = %q, want doc_created", ev.Kind)
	}
	if ev.Slug == nil || *ev.Slug != "stream-doc-1" {
		t.Errorf("slug = %v, want stream-doc-1", ev.Slug)
	}
	if ev.DocID == nil || *ev.DocID != doc.ID {
		t.Errorf("doc_id = %v, want %s", ev.DocID, doc.ID)
	}

	// 2. Reconnect with Last-Event-ID header set to the stream's id: field and verify cursor resumes without repeating
	req2, err := http.NewRequest("GET", ts.URL+"/changes/stream", nil)
	if err != nil {
		t.Fatalf("new request 2: %v", err)
	}
	req2.Header.Set("Last-Event-ID", idLine)
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("stream request 2: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("resumed stream status = %d, want 200", resp2.StatusCode)
	}

	lineChan2 := make(chan string, 100)
	go func() {
		scanner := bufio.NewScanner(resp2.Body)
		for scanner.Scan() {
			lineChan2 <- scanner.Text()
		}
	}()

	// Create second document
	_, err = s.CreateDocument(ctx, "stream-doc-2", "Stream Doc 2", "note", nil, nil, []byte("content 2"), "text/markdown", "tester")
	if err != nil {
		t.Fatalf("CreateDocument 2: %v", err)
	}

	var dataLine2 string
	timeout2 := time.After(3 * time.Second)
	for dataLine2 == "" {
		select {
		case line := <-lineChan2:
			if strings.HasPrefix(line, "data: ") {
				dataLine2 = line
			}
		case <-timeout2:
			t.Fatalf("timed out waiting for second SSE data line")
		}
	}

	var ev2 Event
	if err := json.Unmarshal([]byte(strings.TrimPrefix(dataLine2, "data: ")), &ev2); err != nil {
		t.Fatalf("unmarshal event 2 json: %v", err)
	}
	if ev2.ID <= ev.ID {
		t.Errorf("resumed stream returned repeat or older event ID %d (since was %s)", ev2.ID, idLine)
	}
	if ev2.Slug == nil || *ev2.Slug != "stream-doc-2" {
		t.Errorf("slug 2 = %v, want stream-doc-2", ev2.Slug)
	}
}

func TestEvents_ConcurrentCommitWatermark(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// Transaction A: start transaction, insert an event, do not commit yet.
	txA, err := s.q(ctx).Begin(ctx)
	if err != nil {
		t.Fatalf("Begin txA: %v", err)
	}
	defer txA.Rollback(ctx)

	var docID string
	err = txA.QueryRow(ctx, `
		insert into documents (workspace, slug, title, kind, tags, metadata, created_by, updated_by, fts)
		values (current_setting('app.workspace'), 'doc-a', 'Doc A', 'note', '{}', '{}'::jsonb, 'agent-a', 'agent-a', to_tsvector('english', 'Doc A'))
		returning id`).Scan(&docID)
	if err != nil {
		t.Fatalf("insert doc-a in txA: %v", err)
	}
	_, err = txA.Exec(ctx, `
		insert into events (workspace, kind, doc_id, slug, actor, version)
		values (current_setting('app.workspace'), 'doc_created', $1, 'doc-a', 'agent-a', 1)`, docID)
	if err != nil {
		t.Fatalf("insert event in txA: %v", err)
	}

	// Transaction B: create document & event, and COMMIT immediately.
	_, err = s.CreateDocument(ctx, "doc-b", "Doc B", "note", nil, nil, []byte("b"), "text/markdown", "agent-b")
	if err != nil {
		t.Fatalf("CreateDocument doc-b in txB: %v", err)
	}

	// Read feed from cursor "" while txA is still open.
	evs1, _, err := s.ListEvents(ctx, "", nil, 100)
	if err != nil {
		t.Fatalf("ListEvents 1: %v", err)
	}
	if len(evs1) != 0 {
		t.Fatalf("expected 0 events while txA is in-flight, got %d events (%+v)", len(evs1), evs1)
	}

	// Now commit txA.
	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("Commit txA: %v", err)
	}

	// Read feed again from cursor "". BOTH events must appear now.
	evs2, _, err := s.ListEvents(ctx, "", nil, 100)
	if err != nil {
		t.Fatalf("ListEvents 2: %v", err)
	}
	if len(evs2) != 2 {
		t.Fatalf("expected 2 events after txA commit, got %d (%+v)", len(evs2), evs2)
	}
}

func TestEvents_CascadingFolioDeletes(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// Create folio and two files in it
	_, err := s.CreateDocument(ctx, "myfolio", "My Folio", "folio", nil, nil, nil, "", "tester")
	if err != nil {
		t.Fatalf("CreateDocument folio: %v", err)
	}
	_, err = s.CreateDocument(ctx, "myfolio/file1.md", "File 1", "note", []string{folioTag("myfolio")}, nil, []byte("f1"), "text/markdown", "tester")
	if err != nil {
		t.Fatalf("CreateDocument file1: %v", err)
	}
	_, err = s.CreateDocument(ctx, "myfolio/file2.md", "File 2", "note", []string{folioTag("myfolio")}, nil, []byte("f2"), "text/markdown", "tester")
	if err != nil {
		t.Fatalf("CreateDocument file2: %v", err)
	}

	evs, cursor, err := s.ListEvents(ctx, "", nil, 100)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(evs) != 3 {
		t.Fatalf("expected 3 doc_created events, got %d", len(evs))
	}

	// 1. Cascading soft delete: expect 2 child doc_soft_deleted events + 1 folio doc_soft_deleted event (3 total)
	_, err = s.SoftDeleteDocument(ctx, "myfolio", "tester", true)
	if err != nil {
		t.Fatalf("SoftDeleteDocument cascade: %v", err)
	}

	evsSoft, cursor, err := s.ListEvents(ctx, cursor, nil, 100)
	if err != nil {
		t.Fatalf("ListEvents soft: %v", err)
	}
	if len(evsSoft) != 3 {
		t.Fatalf("expected 3 soft_deleted events (2 files + 1 folio), got %d (%+v)", len(evsSoft), evsSoft)
	}
	softSlugs := map[string]bool{}
	for _, ev := range evsSoft {
		if ev.Kind != "doc_soft_deleted" {
			t.Errorf("expected doc_soft_deleted, got %s", ev.Kind)
		}
		if ev.Slug != nil {
			softSlugs[*ev.Slug] = true
		}
	}
	if !softSlugs["myfolio"] || !softSlugs["myfolio/file1.md"] || !softSlugs["myfolio/file2.md"] {
		t.Errorf("soft delete events missing expected slugs: got %v", softSlugs)
	}

	// Restore folio and files
	_, err = s.RestoreDocument(ctx, "myfolio", "tester")
	if err != nil {
		t.Fatalf("RestoreDocument folio: %v", err)
	}
	_, err = s.RestoreDocument(ctx, "myfolio/file1.md", "tester")
	if err != nil {
		t.Fatalf("RestoreDocument file1: %v", err)
	}
	_, err = s.RestoreDocument(ctx, "myfolio/file2.md", "tester")
	if err != nil {
		t.Fatalf("RestoreDocument file2: %v", err)
	}

	_, cursor, _ = s.ListEvents(ctx, "", nil, 1000)

	// 2. Cascading hard delete: expect 2 child doc_hard_deleted events + 1 folio doc_hard_deleted event (3 total)
	_, err = s.HardDeleteDocument(ctx, "myfolio", "myfolio", "tester", true)
	if err != nil {
		t.Fatalf("HardDeleteDocument cascade: %v", err)
	}

	evsHard, _, err := s.ListEvents(ctx, cursor, nil, 100)
	if err != nil {
		t.Fatalf("ListEvents hard: %v", err)
	}
	if len(evsHard) != 3 {
		t.Fatalf("expected 3 hard_deleted events (2 files + 1 folio), got %d (%+v)", len(evsHard), evsHard)
	}
	hardSlugs := map[string]bool{}
	for _, ev := range evsHard {
		if ev.Kind != "doc_hard_deleted" {
			t.Errorf("expected doc_hard_deleted, got %s", ev.Kind)
		}
		if ev.Slug != nil {
			hardSlugs[*ev.Slug] = true
		}
	}
	if !hardSlugs["myfolio"] || !hardSlugs["myfolio/file1.md"] || !hardSlugs["myfolio/file2.md"] {
		t.Errorf("hard delete events missing expected slugs: got %v", hardSlugs)
	}
}

func TestStreamChanges_IdleHoldsNoConn(t *testing.T) {
	s := testStore(t)
	srv := &Server{store: s, cfg: Config{DefaultWorkspace: "default"}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /changes/stream", srv.auth(srv.streamChanges))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := ts.Client()
	req, err := http.NewRequest("GET", ts.URL+"/changes/stream", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// Sleep 200ms to let initial poll complete and enter idle ticker
	time.Sleep(200 * time.Millisecond)

	acquired := s.db.Stat().AcquiredConns()
	if acquired != 0 {
		t.Fatalf("idle stream holds %d pool connections between polls, want 0", acquired)
	}
}

func TestEvents_CompositeCursorInterleaving(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// 1. TxA: write a doc to acquire xid (e.g. xid 100). Do not insert event or commit yet.
	txA, err := s.q(ctx).Begin(ctx)
	if err != nil {
		t.Fatalf("Begin txA: %v", err)
	}
	defer txA.Rollback(ctx)

	var docIDA string
	err = txA.QueryRow(ctx, `
		insert into documents (workspace, slug, title, kind, tags, metadata, created_by, updated_by, fts)
		values (current_setting('app.workspace'), 'doc-interleave-a', 'Doc A', 'note', '{}', '{}'::jsonb, 'agent-a', 'agent-a', to_tsvector('english', 'Doc A'))
		returning id`).Scan(&docIDA)
	if err != nil {
		t.Fatalf("insert doc-a in txA: %v", err)
	}

	// 2. TxB: start transaction (takes xid 101), insert its event (id=1), and stay open.
	txB, err := s.q(ctx).Begin(ctx)
	if err != nil {
		t.Fatalf("Begin txB: %v", err)
	}
	defer txB.Rollback(ctx)

	var docIDB string
	err = txB.QueryRow(ctx, `
		insert into documents (workspace, slug, title, kind, tags, metadata, created_by, updated_by, fts)
		values (current_setting('app.workspace'), 'doc-interleave-b', 'Doc B', 'note', '{}', '{}'::jsonb, 'agent-b', 'agent-b', to_tsvector('english', 'Doc B'))
		returning id`).Scan(&docIDB)
	if err != nil {
		t.Fatalf("insert doc-b in txB: %v", err)
	}
	_, err = txB.Exec(ctx, `
		insert into events (workspace, kind, doc_id, slug, actor, version)
		values (current_setting('app.workspace'), 'doc_created', $1, 'doc-interleave-b', 'agent-b', 1)`, docIDB)
	if err != nil {
		t.Fatalf("insert event B in txB: %v", err)
	}

	// 3. TxA: now insert its event (gets id=2) and COMMIT.
	_, err = txA.Exec(ctx, `
		insert into events (workspace, kind, doc_id, slug, actor, version)
		values (current_setting('app.workspace'), 'doc_created', $1, 'doc-interleave-a', 'agent-a', 1)`, docIDA)
	if err != nil {
		t.Fatalf("insert event A in txA: %v", err)
	}
	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("Commit txA: %v", err)
	}

	// 4. Reader reads feed with since="".
	evs1, cursor1, err := s.ListEvents(ctx, "", nil, 100)
	if err != nil {
		t.Fatalf("ListEvents 1: %v", err)
	}
	if len(evs1) != 1 || evs1[0].Slug == nil || *evs1[0].Slug != "doc-interleave-a" {
		t.Fatalf("expected 1 event for doc-interleave-a, got %d events (%+v)", len(evs1), evs1)
	}

	// 5. TxB now COMMITS (has id=1).
	if err := txB.Commit(ctx); err != nil {
		t.Fatalf("Commit txB: %v", err)
	}

	// 6. Reader resumes with cursor1.
	evs2, _, err := s.ListEvents(ctx, cursor1, nil, 100)
	if err != nil {
		t.Fatalf("ListEvents 2: %v", err)
	}
	if len(evs2) != 1 || evs2[0].Slug == nil || *evs2[0].Slug != "doc-interleave-b" {
		t.Fatalf("resumed stream with cursor %v skipped event B! got %d events (%+v)", cursor1, len(evs2), evs2)
	}
}

func TestKindsHash_Injective(t *testing.T) {
	// Reviewer's exact counterexample: embedded newline in element vs separate elements
	kinds1 := normalizeKinds([]string{"doc_created", "task_claimed\ntask_completed"})
	kinds2 := normalizeKinds([]string{"doc_created", "task_claimed", "task_completed"})

	hash1 := kindsHash(kinds1)
	hash2 := kindsHash(kinds2)

	if hash1 == hash2 {
		t.Fatalf("kindsHash collision for reviewer counterexample! hash1 = %s, hash2 = %s", hash1, hash2)
	}

	// Empty kinds sentinel vs real kind set
	emptyHash := kindsHash(nil)
	realHash := kindsHash(normalizeKinds([]string{"doc_created"}))
	if emptyHash == realHash {
		t.Fatalf("empty kinds hash collided with real kind set hash!")
	}
	if len(emptyHash) != 64 || len(realHash) != 64 {
		t.Fatalf("hashes must be 64 hex chars, got len %d and %d", len(emptyHash), len(realHash))
	}
}
