package main

import (
	"context"
	"errors"
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
