package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests exercise the lease/CAS state machine and SKIP LOCKED task claiming
// against a real Postgres — the semantics under test (row locks, FOR UPDATE SKIP
// LOCKED, transaction visibility) live in the database, so a fake store would
// assert nothing.
//
// Point TEST_DATABASE_URL at a scratch pgvector database; without it the suite
// skips, so `go test ./...` stays green on a machine with no Postgres:
//
//	make test        # starts a throwaway pgvector container and runs these
//	TEST_DATABASE_URL=postgres://... go test ./...

// testStore opens a Store against TEST_DATABASE_URL with a temp-dir blob backend,
// migrates it, and truncates all state so each test starts clean.
func testStore(t *testing.T) *Store {
	t.Helper()

	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping DB-backed tests (run `make test`)")
	}

	ctx := context.Background()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	if err := db.Ping(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	t.Cleanup(db.Close)

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("signing key: %v", err)
	}
	s := &Store{db: db, blobs: &LocalBlobStore{
		blobDir:    t.TempDir(),
		baseURL:    "http://127.0.0.1:8770",
		signingKey: key,
	}}

	if err := s.migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// Truncate rather than recreate: migrations are the expensive part, and the
	// cascade clears revisions/locks with the documents that own them.
	if _, err := db.Exec(ctx,
		`truncate documents, doc_locks, document_revisions, tasks, actors restart identity cascade`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s
}

// newDoc creates a document and returns it, failing the test on error.
func newDoc(t *testing.T, s *Store, slug string) *Document {
	t.Helper()
	doc, err := s.CreateDocument(context.Background(), slug, "Test "+slug, "note",
		[]string{"test"}, json.RawMessage(`{}`), nil, "text/markdown", "tester")
	if err != nil {
		t.Fatalf("create document %q: %v", slug, err)
	}
	return doc
}

// leaseFor acquires a fresh lease and returns its token.
func leaseFor(t *testing.T, s *Store, docID, owner string, ttl time.Duration) string {
	t.Helper()
	l, err := s.AcquireLease(context.Background(), docID, owner, "test", ttl, "")
	if err != nil {
		t.Fatalf("acquire lease for %s as %s: %v", docID, owner, err)
	}
	return l.LeaseToken
}

// readContent returns the bytes behind a document's content key.
func readContent(t *testing.T, s *Store, key string) []byte {
	t.Helper()
	rc, err := s.GetContent(context.Background(), key)
	if err != nil {
		t.Fatalf("get content %q: %v", key, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read content %q: %v", key, err)
	}
	return b
}
