package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Sentinel errors mapped to HTTP status codes by the handlers.
var (
	ErrNotFound            = errors.New("not found")
	ErrAlreadyExists       = errors.New("already exists")
	ErrLeaseHeld           = errors.New("lease held by another agent")
	ErrNoLease             = errors.New("caller does not hold a valid lease")
	ErrVersionConflict     = errors.New("version conflict")
	ErrBadTaskStatus       = errors.New("status must be 'done' or 'failed'")
	ErrNotClaimant         = errors.New("task is not claimed by this actor")
	ErrNotClaimable        = errors.New("task is not claimable")
	ErrDeleted             = errors.New("document is soft-deleted; restore it first")
	ErrConfirmMismatch     = errors.New("hard-delete confirm must exactly equal the document slug")
	ErrFolioNotEmpty       = errors.New("folio still has files; pass cascade=true or delete files first")
	ErrNoWorkspace         = errors.New("unknown workspace; create it with create_workspace first")
	ErrBadWorkspace        = errors.New("workspace must match [a-z0-9][a-z0-9_-]{0,62}")
	ErrBadCursor           = errors.New("invalid cursor format (expected '<workspace>:<kindshash>:<xact_id>:<id>')")
	ErrCursorScopeMismatch = errors.New("cursor scope mismatch")
)

// normalizeCreateError turns Postgres's unique-index error into a stable
// domain error. Callers can then safely distinguish an idempotent create from
// a real server failure without depending on a database error string.
func normalizeCreateError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrAlreadyExists
	}
	return err
}

type Document struct {
	ID          string          `json:"id"`
	Slug        string          `json:"slug"`
	Title       string          `json:"title"`
	Kind        string          `json:"kind"`
	ContentKey  string          `json:"content_key,omitempty"`
	ContentHash string          `json:"content_hash,omitempty"`
	ContentType string          `json:"content_type"`
	SizeBytes   int64           `json:"size_bytes"`
	Tags        []string        `json:"tags"`
	Metadata    json.RawMessage `json:"metadata"`
	Version     int             `json:"version"`
	CreatedBy   string          `json:"created_by,omitempty"`
	UpdatedBy   string          `json:"updated_by,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	DeletedAt   *time.Time      `json:"deleted_at,omitempty"`
	DeletedBy   string          `json:"deleted_by,omitempty"`
}

type Lease struct {
	DocumentID string    `json:"document_id"`
	Owner      string    `json:"owner"`
	LeaseToken string    `json:"lease_token,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	AcquiredAt time.Time `json:"acquired_at"`
	RenewedAt  time.Time `json:"renewed_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type Event struct {
	ID        int64     `json:"id"`
	TS        time.Time `json:"ts"`
	Workspace string    `json:"workspace"`
	Kind      string    `json:"kind"`
	DocID     *string   `json:"doc_id,omitempty"`
	Slug      *string   `json:"slug,omitempty"`
	Actor     *string   `json:"actor,omitempty"`
	Version   *int      `json:"version,omitempty"`
	TaskID    *string   `json:"task_id,omitempty"`
	XactID    string    `json:"xact_id,omitempty"`
}

type BlobStore interface {
	PutObject(ctx context.Context, key string, data []byte, contentType string) error
	GetObject(ctx context.Context, key string) (io.ReadCloser, error)
	PresignGetObject(ctx context.Context, key string, ttl time.Duration) (string, error)
}

type S3BlobStore struct {
	client *minio.Client
	bucket string
	// Content URLs point back at tracker rather than at the bucket. S3_ENDPOINT is
	// the address *tracker* uses to reach S3 — usually loopback or a compose service
	// name — and SigV4 binds it into the signature, so a presigned URL cannot be
	// rewritten for the caller afterwards. Serving blobs through BASE_URL keeps one
	// reachable URL scheme for both backends and leaves the bucket unexposed.
	baseURL    string
	signingKey []byte
}

func (s *S3BlobStore) PutObject(ctx context.Context, key string, data []byte, contentType string) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: contentType})
	return err
}

func (s *S3BlobStore) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
}

func (s *S3BlobStore) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (string, error) {
	return signedBlobURL(s.baseURL, s.signingKey, key, ttl), nil
}

type LocalBlobStore struct {
	blobDir    string
	baseURL    string
	signingKey []byte
}

// blobSig is the HMAC over a blob key + expiry, making local content URLs
// expiring capabilities (the /blobs route verifies it when auth is enabled).
func blobSig(key []byte, blobKey string, exp int64) string {
	m := hmac.New(sha256.New, key)
	fmt.Fprintf(m, "%s|%d", blobKey, exp)
	return hex.EncodeToString(m.Sum(nil))
}

// signedBlobURL builds the expiring /blobs/ capability both backends hand out as
// content_url. It is always rooted at BASE_URL, so the URL is reachable from
// wherever tracker itself is reachable — never only from the server's own host.
func signedBlobURL(baseURL string, signingKey []byte, blobKey string, ttl time.Duration) string {
	exp := time.Now().Add(ttl).Unix()
	return fmt.Sprintf("%s/blobs/%s?e=%d&s=%s",
		strings.TrimRight(baseURL, "/"), blobKey, exp, blobSig(signingKey, blobKey, exp))
}

func (l *LocalBlobStore) PutObject(ctx context.Context, key string, data []byte, contentType string) error {
	path := filepath.Join(l.blobDir, key)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (l *LocalBlobStore) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(l.blobDir, key))
}

func (l *LocalBlobStore) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (string, error) {
	return signedBlobURL(l.baseURL, l.signingKey, key, ttl), nil
}

type Store struct {
	db    *pgxpool.Pool
	blobs BlobStore
}

// AllWorkspaces lifts workspace scoping for maintenance paths that must see the
// whole store — migrate-blobs has to enumerate every blob, not one workspace's.
// It can never arrive from a request: validWorkspace rejects it, and that is the
// only route by which a caller-supplied name reaches Scoped.
const AllWorkspaces = "*"

// Workspace names are constrained so they are safe to display, embed in a slug
// path, and compare exactly — and so the AllWorkspaces sentinel stays
// unreachable from user input.
var wsNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

func validWorkspace(name string) bool { return wsNameRe.MatchString(name) }

// querier is the slice of pgx shared by *pgxpool.Pool and *pgxpool.Conn, so a
// Store method can run against either without caring which it got.
type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Begin(context.Context) (pgx.Tx, error)
}

type wsConnKey struct{}

// q returns the workspace-pinned connection installed by Scoped, falling back to
// the bare pool. The fallback is only right for statements RLS does not govern
// (migrations, Ping). Everything else goes through Scoped — and a path that
// forgets to gets a connection with app.workspace unset, which the policies read
// as "no rows" rather than "every row". Forgetting fails closed.
func (s *Store) q(ctx context.Context) querier {
	if c, ok := ctx.Value(wsConnKey{}).(*pgxpool.Conn); ok {
		return c
	}
	return s.db
}

// Scoped pins one pooled connection to a workspace for the life of a request and
// returns it inside a derived context, along with the func that releases it.
//
// The setting is session-scoped rather than transaction-scoped (set_config's
// third argument is false) because Store methods issue both bare queries and
// their own transactions; pinning the connection covers both without every
// method having to open a transaction it does not otherwise need.
func (s *Store) Scoped(ctx context.Context, workspace string) (context.Context, func(), error) {
	noop := func() {}
	c, err := s.db.Acquire(ctx)
	if err != nil {
		return ctx, noop, err
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			// Restore the connection before it returns to the pool. Every request sets
			// both of these on acquire, so a leftover could not actually be observed —
			// this is belt and braces for a future path that forgets. WithoutCancel so
			// the reset still runs when the client hung up mid-request.
			_, _ = c.Exec(context.WithoutCancel(ctx),
				`reset role; select set_config('app.workspace', '', false)`)
			c.Release()
		})
	}
	// Drop to the unprivileged role before touching any data. Without this the
	// policies are inert: tracker normally connects as the database superuser
	// created by the postgres image, and a superuser bypasses RLS no matter what
	// ENABLE/FORCE say. Migrations still run as the real user, on the bare pool.
	if _, err := c.Exec(ctx, `set role tracker_agent`); err != nil {
		release()
		return ctx, noop, fmt.Errorf("assume tracker_agent: %w", err)
	}
	// One round trip: install the setting and confirm the workspace exists. An
	// unknown name would otherwise read as an empty workspace and only fail later,
	// on a foreign-key violation from the first write.
	var known bool
	var ignored string
	err = c.QueryRow(ctx,
		`select set_config('app.workspace', $1, false), exists(select 1 from workspaces where name = $1)`,
		workspace).Scan(&ignored, &known)
	if err != nil {
		release()
		return ctx, noop, err
	}
	if !known && workspace != AllWorkspaces {
		release()
		return ctx, noop, ErrNoWorkspace
	}
	return context.WithValue(ctx, wsConnKey{}, c), release, nil
}

// Workspace is the registry row describing one partition of the store.
type Workspace struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedBy   string    `json:"created_by,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// CreateWorkspace registers a workspace. Creation is deliberately explicit: with
// auto-create, a typo'd X-Workspace would silently open an empty world instead of
// reporting that the name is unknown.
func (s *Store) CreateWorkspace(ctx context.Context, name, description, by string) (*Workspace, error) {
	if !validWorkspace(name) {
		return nil, ErrBadWorkspace
	}
	// Registry writes are not workspace-scoped, so they run on the pool: a caller
	// in workspace A must be able to create workspace B.
	var ws Workspace
	err := s.db.QueryRow(ctx, `
		insert into workspaces (name, description, created_by) values ($1, $2, $3)
		on conflict (name) do nothing
		returning name, description, coalesce(created_by,''), created_at`,
		name, description, by).Scan(&ws.Name, &ws.Description, &ws.CreatedBy, &ws.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrAlreadyExists
	}
	if err != nil {
		return nil, err
	}
	return &ws, nil
}

// ListWorkspaces returns the registry. Not workspace-scoped by design — this is
// how an agent discovers where it is allowed to work.
func (s *Store) ListWorkspaces(ctx context.Context) ([]Workspace, error) {
	rows, err := s.db.Query(ctx,
		`select name, description, coalesce(created_by,''), created_at from workspaces order by name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Workspace{}
	for rows.Next() {
		var w Workspace
		if err := rows.Scan(&w.Name, &w.Description, &w.CreatedBy, &w.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func openStore(ctx context.Context, cfg Config) (*Store, error) {
	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := db.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	blobs, err := buildBlobStore(ctx, cfg, cfg.StorageType, cfg.BlobDir)
	if err != nil {
		return nil, err
	}
	st := &Store{db: db, blobs: blobs}
	if err := st.migrate(ctx); err != nil {
		return nil, err
	}
	return st, nil
}

// buildBlobStore constructs a specific blob backend ("file" or "s3") from cfg.
// blobDir overrides cfg.BlobDir for the file backend, so migrate-blobs can point
// a destination at a directory other than the active one.
func buildBlobStore(ctx context.Context, cfg Config, storageType, blobDir string) (BlobStore, error) {
	if storageType == "file" {
		if blobDir == "" {
			blobDir = cfg.BlobDir
		}
		if err := os.MkdirAll(blobDir, 0o755); err != nil {
			return nil, fmt.Errorf("create blob dir %q: %w", blobDir, err)
		}
		return &LocalBlobStore{blobDir: blobDir, baseURL: cfg.BaseURL, signingKey: cfg.BlobSigningKey}, nil
	}
	s3Client, err := minio.New(cfg.S3Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.S3AccessKey, cfg.S3SecretKey, ""),
		Secure: cfg.S3UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("init s3: %w", err)
	}
	exists, err := s3Client.BucketExists(ctx, cfg.S3Bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		if err := s3Client.MakeBucket(ctx, cfg.S3Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("create bucket %q: %w", cfg.S3Bucket, err)
		}
	}
	return &S3BlobStore{
		client:     s3Client,
		bucket:     cfg.S3Bucket,
		baseURL:    cfg.BaseURL,
		signingKey: cfg.BlobSigningKey,
	}, nil
}

// BlobRef is a referenced content blob: its key plus a best-effort content type
// (from a current document that uses it, else a generic default).
type BlobRef struct {
	Key         string
	ContentType string
}

// AllBlobRefs lists every distinct blob key referenced by a live document or any
// revision — the authoritative set to migrate (and naturally excludes orphans).
func (s *Store) AllBlobRefs(ctx context.Context) ([]BlobRef, error) {
	rows, err := s.q(ctx).Query(ctx, `
		select distinct on (t.content_key)
			t.content_key, coalesce(d.content_type, 'application/octet-stream')
		from (
			select content_key from documents where content_key <> ''
			union
			select content_key from document_revisions where content_key <> ''
		) t
		left join documents d on d.content_key = t.content_key
		order by t.content_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []BlobRef{}
	for rows.Next() {
		var b BlobRef
		if err := rows.Scan(&b.Key, &b.ContentType); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) migrate(ctx context.Context) error {
	files, err := fs.Glob(migrationsFS, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files) // 0001_, 0002_, ... applied in order; each is idempotent
	for _, f := range files {
		sql, err := migrationsFS.ReadFile(f)
		if err != nil {
			return err
		}
		// DDL, and RLS does not govern it — this must run on the bare pool.
		if _, err := s.db.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("apply %s: %w", f, err)
		}
	}
	return nil
}

// docSelect is the column list shared by single/list reads.
const docSelect = `id, slug, title, kind, coalesce(content_key,''), coalesce(content_hash,''),
	content_type, size_bytes, tags, metadata, version, coalesce(created_by,''), coalesce(updated_by,''),
	created_at, updated_at, deleted_at, coalesce(deleted_by,'')`

func scanDoc(row pgx.Row) (*Document, error) {
	var d Document
	err := row.Scan(&d.ID, &d.Slug, &d.Title, &d.Kind, &d.ContentKey, &d.ContentHash,
		&d.ContentType, &d.SizeBytes, &d.Tags, &d.Metadata, &d.Version, &d.CreatedBy, &d.UpdatedBy,
		&d.CreatedAt, &d.UpdatedAt, &d.DeletedAt, &d.DeletedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &d, err
}

// Deleted reports whether the document carries a soft-delete tombstone.
func (d *Document) Deleted() bool { return d != nil && d.DeletedAt != nil }

// CreateDocument inserts a new document. metadata (jsonb, may be nil) holds
// caller-defined fields (e.g. a folio's description/source). If content is
// non-nil it is seeded as version 1 — safe without a lease since a brand-new
// document has no concurrent writer to conflict with.
func (s *Store) CreateDocument(ctx context.Context, slug, title, kind string, tags []string, metadata json.RawMessage, content []byte, contentType, by string) (*Document, error) {
	if kind == "" {
		kind = "note"
	}
	if tags == nil {
		tags = []string{}
	}
	if len(metadata) == 0 {
		metadata = json.RawMessage(`{}`)
	}
	if contentType == "" {
		contentType = "text/markdown"
	}

	if content == nil {
		tx, err := s.q(ctx).Begin(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback(ctx)

		row := tx.QueryRow(ctx, `
			insert into documents (workspace, slug, title, kind, tags, metadata, created_by, updated_by, fts)
			values (current_setting('app.workspace'), $1, $2, $3, $4, $5::jsonb, $6, $6, to_tsvector('english', $2))
			returning `+docSelect, slug, title, kind, tags, string(metadata), by)
		doc, err := scanDoc(row)
		if err != nil {
			return nil, normalizeCreateError(err)
		}
		if _, err := tx.Exec(ctx, `
			insert into events (workspace, kind, doc_id, slug, actor, version)
			values (current_setting('app.workspace'), 'doc_created', $1, $2, $3, $4)`,
			doc.ID, doc.Slug, by, doc.Version); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		s.touchActor(ctx, by)
		return doc, nil
	}

	// Seed initial content: blob first (content-addressed), then the row + v1 revision.
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	key := "sha256/" + hash
	if err := s.blobs.PutObject(ctx, key, content, contentType); err != nil {
		return nil, fmt.Errorf("put blob: %w", err)
	}

	tx, err := s.q(ctx).Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		insert into documents (workspace, slug, title, kind, tags, metadata, content_key, content_hash,
			content_type, size_bytes, version, created_by, updated_by,
			fts)
		values (current_setting('app.workspace'), $1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, 1, $10, $10,
			to_tsvector('english', $2 || ' ' || left($11, 100000)))
		returning `+docSelect, slug, title, kind, tags, string(metadata), key, hash,
		contentType, int64(len(content)), by, string(content))
	doc, err := scanDoc(row)
	if err != nil {
		return nil, normalizeCreateError(err)
	}
	if _, err := tx.Exec(ctx, `
		insert into document_revisions (document_id, version, content_key, content_hash, size_bytes, author)
		values ($1, 1, $2, $3, $4, $5)`, doc.ID, key, hash, int64(len(content)), by); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		insert into events (workspace, kind, doc_id, slug, actor, version)
		values (current_setting('app.workspace'), 'doc_created', $1, $2, $3, $4)`,
		doc.ID, doc.Slug, by, doc.Version); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.touchActor(ctx, by)
	return doc, nil
}

// idClause matches either a UUID id or a slug, so callers can use whichever.
const idClause = `(id::text = $1 or slug = $1)`

func (s *Store) GetDocument(ctx context.Context, idOrSlug string) (*Document, error) {
	return scanDoc(s.q(ctx).QueryRow(ctx, `select `+docSelect+` from documents where `+idClause, idOrSlug))
}

// ListDocuments returns a page of documents matching the filters plus the total
// match count (for pagination). limit defaults to 50 (cap 200) when <= 0.
//
// mode selects the full-text query parser: "" / "web" uses websearch_to_tsquery
// (understands quoted "phrases", OR, and -negation, like a search box); "plain"
// (or "and") uses plainto_tsquery, which ANDs every term. When a query is
// present, results are ranked by lexical relevance with a gentle recency decay
// rather than strict recency.
//
// deleted selects the soft-delete filter: ""/"exclude" (default) hides tombstones,
// "only" returns soft-deleted docs, "include" returns both.
func (s *Store) ListDocuments(ctx context.Context, q, kind, tag, mode, deleted string, limit, offset int) ([]Document, int, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	tsq := `websearch_to_tsquery('english', $1)`
	if mode == "plain" || mode == "and" {
		tsq = `plainto_tsquery('english', $1)`
	}
	delFilter := `and deleted_at is null`
	switch deleted {
	case "only":
		delFilter = `and deleted_at is not null`
	case "include":
		delFilter = ``
	}

	filter := `where ($1 = '' or fts @@ ` + tsq + `)
		  and ($2 = '' or kind = $2)
		  and ($3 = '' or $3 = any(tags)) ` + delFilter

	var total int
	if err := s.q(ctx).QueryRow(ctx, `select count(*) from documents `+filter, q, kind, tag).Scan(&total); err != nil {
		return nil, 0, err
	}

	// Newest-first when browsing; relevance (with ~30-day recency e-fold) when searching.
	order := `order by updated_at desc`
	if q != "" {
		order = `order by ts_rank(fts, ` + tsq + `) * exp(-extract(epoch from (now() - updated_at)) / 2592000.0) desc, updated_at desc`
	}
	rows, err := s.q(ctx).Query(ctx, `select `+docSelect+` from documents `+filter+` `+order+`
		limit $4 offset $5`, q, kind, tag, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := []Document{}
	for rows.Next() {
		d, err := scanDoc(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, *d)
	}
	return out, total, rows.Err()
}

// DocPatch carries label-only changes: tags, metadata, title, and kind.
// These are NOT content, so applying them never bumps the version, rewrites a
// blob, or requires a lease.
type DocPatch struct {
	Tags       []string        // full replacement of the tag set when non-nil
	AddTags    []string        // tags to add (set union)
	RemoveTags []string        // tags to drop
	Metadata   json.RawMessage // shallow-merged into existing metadata when non-nil
	Title      *string         // replaces the title when non-nil
	Kind       *string         // replaces the kind when non-nil
}

// PatchDocument mutates a document's labels (tags / metadata / title / kind) without
// touching its content. Deliberately does not bump version, rewrite the blob, or
// require a lease — tags and metadata are labels, not content. (fts is left as-is;
// a changed title reindexes on the next content write.) Requires only an actor
// for attribution.
func (s *Store) PatchDocument(ctx context.Context, idOrSlug string, p DocPatch, by string) (*Document, error) {
	tx, err := s.q(ctx).Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var id, title, kind, contentKey string
	var tags []string
	var meta json.RawMessage
	var deletedAt *time.Time
	err = tx.QueryRow(ctx, `select id, title, kind, tags, metadata, coalesce(content_key,''), deleted_at from documents where `+idClause+` for update`, idOrSlug).
		Scan(&id, &title, &kind, &tags, &meta, &contentKey, &deletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	if deletedAt != nil {
		return nil, ErrDeleted
	}

	if p.Tags != nil { // explicit full replace wins
		tags = p.Tags
	}
	tags = removeTags(tags, p.RemoveTags)
	tags = addTags(tags, p.AddTags)
	if tags == nil {
		tags = []string{}
	}
	if len(p.Metadata) > 0 {
		meta = withMeta(meta, rawToMap(p.Metadata))
	}
	if p.Kind != nil && strings.TrimSpace(*p.Kind) != "" {
		kind = strings.TrimSpace(*p.Kind)
	}
	ftsSet := ``
	args := []any{id, tags, string(meta), title, kind, by}
	if p.Title != nil && *p.Title != title {
		// A renamed doc must be findable under its NEW title, so reindex fts —
		// which means re-reading the content text the index also covers.
		title = *p.Title
		ftsInput := title
		if contentKey != "" {
			text, err := s.readBlobText(ctx, contentKey, 100000)
			if err != nil {
				return nil, err
			}
			ftsInput += " " + text
		}
		ftsSet = `, fts = to_tsvector('english', $7)`
		args = []any{id, tags, string(meta), title, kind, by, ftsInput}
	}

	row := tx.QueryRow(ctx, `
		update documents set tags = $2, metadata = $3::jsonb, title = $4, kind = $5,
			updated_by = $6, updated_at = now()`+ftsSet+`
		where id = $1
		returning `+docSelect, args...)
	doc, err := scanDoc(row)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		insert into events (workspace, kind, doc_id, slug, actor, version)
		values (current_setting('app.workspace'), 'doc_retagged', $1, $2, $3, $4)`,
		doc.ID, doc.Slug, by, doc.Version); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.touchActor(ctx, by)
	return doc, nil
}

func addTags(cur, add []string) []string {
	seen := map[string]bool{}
	for _, t := range cur {
		seen[t] = true
	}
	for _, t := range add {
		if t != "" && !seen[t] {
			cur = append(cur, t)
			seen[t] = true
		}
	}
	return cur
}

func removeTags(cur, rm []string) []string {
	if len(rm) == 0 {
		return cur
	}
	drop := map[string]bool{}
	for _, t := range rm {
		drop[t] = true
	}
	out := make([]string, 0, len(cur))
	for _, t := range cur {
		if !drop[t] {
			out = append(out, t)
		}
	}
	return out
}

func rawToMap(raw json.RawMessage) map[string]any {
	m := map[string]any{}
	_ = json.Unmarshal(raw, &m)
	return m
}

// TagCount is one distinct tag and how many documents carry it.
type TagCount struct {
	Tag   string `json:"tag"`
	Count int    `json:"count"`
}

// ListTags enumerates the whole tag vocabulary with usage counts, so an agent
// can discover what tags exist without pulling every document.
func (s *Store) ListTags(ctx context.Context) ([]TagCount, error) {
	rows, err := s.q(ctx).Query(ctx, `
		select t, count(*) from documents, unnest(tags) as t
		where deleted_at is null
		group by t order by count(*) desc, t`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TagCount{}
	for rows.Next() {
		var tc TagCount
		if err := rows.Scan(&tc.Tag, &tc.Count); err != nil {
			return nil, err
		}
		out = append(out, tc)
	}
	return out, rows.Err()
}

// AcquireLease acquires, renews (when leaseToken matches the live holder), or
// steals (when the existing lease has expired) the write-lease on a document.
// Returns ErrLeaseHeld (with the current holder) if a different agent holds a
// live lease.
func (s *Store) AcquireLease(ctx context.Context, docID, owner, reason string, ttl time.Duration, leaseToken string) (*Lease, error) {
	tx, err := s.q(ctx).Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Ensure the document exists and serialize concurrent lock attempts.
	var realID string
	var deletedAt *time.Time
	err = tx.QueryRow(ctx, `select id, deleted_at from documents where `+idClause+` for update`, docID).Scan(&realID, &deletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	if deletedAt != nil {
		return nil, ErrDeleted
	}

	var cur Lease
	var found bool
	err = tx.QueryRow(ctx, `
		select document_id, owner, lease_token::text, coalesce(reason,''), acquired_at, renewed_at, expires_at
		from doc_locks where document_id = $1 for update`, realID).
		Scan(&cur.DocumentID, &cur.Owner, &cur.LeaseToken, &cur.Reason, &cur.AcquiredAt, &cur.RenewedAt, &cur.ExpiresAt)
	if err == nil {
		found = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	now := time.Now()
	expires := now.Add(ttl)

	switch {
	case found && cur.ExpiresAt.After(now) && cur.LeaseToken == leaseToken && leaseToken != "":
		// Renew: same holder, valid token. RETURNING the new timestamps matters —
		// without it the caller gets back the pre-renewal expires_at and can
		// believe a freshly renewed lease is about to lapse.
		err = tx.QueryRow(ctx, `
			update doc_locks set renewed_at = now(), expires_at = $2, reason = $3
			where document_id = $1
			returning acquired_at, renewed_at, expires_at`, realID, expires, reason).
			Scan(&cur.AcquiredAt, &cur.RenewedAt, &cur.ExpiresAt)
	case found && cur.ExpiresAt.After(now):
		// Live lease held by someone else (or no/wrong token): denied.
		return &cur, ErrLeaseHeld
	case found:
		// Existing lease expired: steal it with a fresh token.
		err = tx.QueryRow(ctx, `
			update doc_locks set owner = $2, lease_token = gen_random_uuid(), reason = $3,
				acquired_at = now(), renewed_at = now(), expires_at = $4
			where document_id = $1
			returning lease_token::text, acquired_at, renewed_at, expires_at`, realID, owner, reason, expires).
			Scan(&cur.LeaseToken, &cur.AcquiredAt, &cur.RenewedAt, &cur.ExpiresAt)
	default:
		// No lease yet: insert fresh.
		err = tx.QueryRow(ctx, `
			insert into doc_locks (document_id, owner, reason, expires_at)
			values ($1, $2, $3, $4)
			returning lease_token::text, acquired_at, renewed_at, expires_at`, realID, owner, reason, expires).
			Scan(&cur.LeaseToken, &cur.AcquiredAt, &cur.RenewedAt, &cur.ExpiresAt)
	}
	if err != nil {
		return nil, err
	}
	cur.DocumentID, cur.Owner, cur.Reason = realID, owner, reason
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.touchActor(ctx, owner)
	return &cur, nil
}

// GetLease returns the current lease for a doc and whether it is still live.
func (s *Store) GetLease(ctx context.Context, docID string) (*Lease, bool, error) {
	var l Lease
	err := s.q(ctx).QueryRow(ctx, `
		select dl.document_id, dl.owner, coalesce(dl.reason,''), dl.acquired_at, dl.renewed_at, dl.expires_at
		from doc_locks dl join documents d on d.id = dl.document_id
		where `+idClause+`
	`, docID).Scan(&l.DocumentID, &l.Owner, &l.Reason, &l.AcquiredAt, &l.RenewedAt, &l.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &l, l.ExpiresAt.After(time.Now()), nil
}

// ReleaseLease drops the lease iff the caller presents the matching token.
func (s *Store) ReleaseLease(ctx context.Context, docID, leaseToken string) error {
	ct, err := s.q(ctx).Exec(ctx, `
		delete from doc_locks
		where document_id = (select id from documents where `+idClause+`)
		  and lease_token::text = $2`, docID, leaseToken)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrNoLease
	}
	return nil
}

// WriteContent stores bytes in the blob store (content-addressed) then atomically bumps
// the document version, requiring (a) a live lease held by the caller and
// (b) baseVersion to match the current version (optimistic CAS).
func (s *Store) WriteContent(ctx context.Context, docID, owner, leaseToken string, baseVersion int, contentType string, data []byte) (*Document, error) {
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	key := "sha256/" + hash

	// Upload first; blobs are immutable and content-addressed, so a re-PUT of the
	// same bytes is harmless and a failed DB step only leaves a GC-able orphan.
	err := s.blobs.PutObject(ctx, key, data, contentType)
	if err != nil {
		return nil, fmt.Errorf("put blob: %w", err)
	}

	tx, err := s.q(ctx).Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var id string
	var version int
	var deletedAt *time.Time
	err = tx.QueryRow(ctx, `select id, version, deleted_at from documents where `+idClause+` for update`, docID).Scan(&id, &version, &deletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	if deletedAt != nil {
		return nil, ErrDeleted
	}

	// Require a live lease, held by THIS actor, with a matching token.
	var lockTok, lockOwner string
	var exp time.Time
	err = tx.QueryRow(ctx, `select lease_token::text, owner, expires_at from doc_locks where document_id = $1`, id).Scan(&lockTok, &lockOwner, &exp)
	if errors.Is(err, pgx.ErrNoRows) || exp.Before(time.Now()) || lockTok != leaseToken || leaseToken == "" || lockOwner != owner {
		return nil, ErrNoLease
	} else if err != nil {
		return nil, err
	}

	if baseVersion != version {
		return nil, fmt.Errorf("%w: have %d, expected %d", ErrVersionConflict, version, baseVersion)
	}

	row := tx.QueryRow(ctx, `
		update documents set content_key = $2, content_hash = $3, size_bytes = $4,
			content_type = $5, version = version + 1, updated_by = $6, updated_at = now(),
			fts = to_tsvector('english', title || ' ' || left($7, 100000))
		where id = $1
		returning `+docSelect, id, key, hash, int64(len(data)), contentType, owner, string(data))
	doc, err := scanDoc(row)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		insert into document_revisions (document_id, version, content_key, content_hash, size_bytes, author)
		values ($1, $2, $3, $4, $5, $6)`, id, doc.Version, key, hash, int64(len(data)), owner); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		insert into events (workspace, kind, doc_id, slug, actor, version)
		values (current_setting('app.workspace'), 'doc_updated', $1, $2, $3, $4)`,
		doc.ID, doc.Slug, owner, doc.Version); err != nil {
		return nil, err
	}
	// Heartbeat the lease on a successful write.
	_, _ = tx.Exec(ctx, `update doc_locks set renewed_at = now() where document_id = $1`, id)

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.touchActor(ctx, owner)
	return doc, nil
}

// SoftDeleteDocument marks a document deleted without removing its row or
// revisions. Default search excludes it; get-by-id and deleted=only|include still
// find it. Folios with live files refuse unless cascade=true (which soft-deletes
// those files too). Idempotent if already soft-deleted.
func (s *Store) SoftDeleteDocument(ctx context.Context, idOrSlug, by string, cascade bool) (*Document, error) {
	tx, err := s.q(ctx).Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	doc, err := scanDoc(tx.QueryRow(ctx, `select `+docSelect+` from documents where `+idClause+` for update`, idOrSlug))
	if err != nil {
		return nil, err
	}
	if doc.Deleted() {
		return doc, nil
	}
	if doc.Kind == "folio" {
		var n int
		if err := tx.QueryRow(ctx, `
			select count(*) from documents
			where $1 = any(tags) and deleted_at is null and id <> $2`,
			folioTag(doc.Slug), doc.ID).Scan(&n); err != nil {
			return nil, err
		}
		if n > 0 && !cascade {
			return nil, ErrFolioNotEmpty
		}
		if cascade && n > 0 {
			if _, err := tx.Exec(ctx, `
				with changed as (
					update documents set deleted_at = now(), deleted_by = $1, updated_by = $1, updated_at = now()
					where $2 = any(tags) and deleted_at is null and id <> $3
					returning id, slug, version
				)
				insert into events (workspace, kind, doc_id, slug, actor, version)
				select current_setting('app.workspace'), 'doc_soft_deleted', id, slug, $1, version from changed`,
				by, folioTag(doc.Slug), doc.ID); err != nil {
				return nil, err
			}
			// Drop leases on cascaded files.
			if _, err := tx.Exec(ctx, `
				delete from doc_locks
				where document_id in (
					select id from documents where $1 = any(tags) and id <> $2
				)`, folioTag(doc.Slug), doc.ID); err != nil {
				return nil, err
			}
		}
	}

	row := tx.QueryRow(ctx, `
		update documents set deleted_at = now(), deleted_by = $2, updated_by = $2, updated_at = now()
		where id = $1
		returning `+docSelect, doc.ID, by)
	out, err := scanDoc(row)
	if err != nil {
		return nil, err
	}
	_, _ = tx.Exec(ctx, `delete from doc_locks where document_id = $1`, doc.ID)
	if _, err := tx.Exec(ctx, `
		insert into events (workspace, kind, doc_id, slug, actor, version)
		values (current_setting('app.workspace'), 'doc_soft_deleted', $1, $2, $3, $4)`,
		out.ID, out.Slug, by, out.Version); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.touchActor(ctx, by)
	return out, nil
}

// RestoreDocument clears a soft-delete tombstone. No-op if the doc is live.
func (s *Store) RestoreDocument(ctx context.Context, idOrSlug, by string) (*Document, error) {
	tx, err := s.q(ctx).Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		update documents set deleted_at = null, deleted_by = null, updated_by = $2, updated_at = now()
		where `+idClause+`
		returning `+docSelect, idOrSlug, by)
	doc, err := scanDoc(row)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		insert into events (workspace, kind, doc_id, slug, actor, version)
		values (current_setting('app.workspace'), 'doc_restored', $1, $2, $3, $4)`,
		doc.ID, doc.Slug, by, doc.Version); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.touchActor(ctx, by)
	return doc, nil
}

// HardDeleteDocument permanently removes a document row (revisions and locks
// cascade). Blobs are left for later GC. confirm must exactly equal the
// document's current slug — the mandatory extra confirmation for irreversible
// deletes. Folios with any remaining files (including soft-deleted) refuse
// unless cascade=true.
func (s *Store) HardDeleteDocument(ctx context.Context, idOrSlug, confirm, by string, cascade bool) (*Document, error) {
	tx, err := s.q(ctx).Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	doc, err := scanDoc(tx.QueryRow(ctx, `select `+docSelect+` from documents where `+idClause+` for update`, idOrSlug))
	if err != nil {
		return nil, err
	}
	if confirm != doc.Slug {
		return nil, fmt.Errorf("%w (got %q, want %q)", ErrConfirmMismatch, confirm, doc.Slug)
	}
	if doc.Kind == "folio" {
		var n int
		if err := tx.QueryRow(ctx, `
			select count(*) from documents where $1 = any(tags) and id <> $2`,
			folioTag(doc.Slug), doc.ID).Scan(&n); err != nil {
			return nil, err
		}
		if n > 0 && !cascade {
			return nil, ErrFolioNotEmpty
		}
		if cascade && n > 0 {
			if _, err := tx.Exec(ctx, `
				with changed as (
					delete from documents
					where $2 = any(tags) and id <> $3
					returning id, slug, version
				)
				insert into events (workspace, kind, doc_id, slug, actor, version)
				select current_setting('app.workspace'), 'doc_hard_deleted', id, slug, $1, version from changed`,
				by, folioTag(doc.Slug), doc.ID); err != nil {
				return nil, err
			}
		}
	}
	if _, err := tx.Exec(ctx, `
		insert into events (workspace, kind, doc_id, slug, actor, version)
		values (current_setting('app.workspace'), 'doc_hard_deleted', $1, $2, $3, $4)`,
		doc.ID, doc.Slug, by, doc.Version); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `delete from documents where id = $1`, doc.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.touchActor(ctx, by)
	return doc, nil
}

// readBlobText reads up to limit bytes of a blob as text (for fts reindexing).
func (s *Store) readBlobText(ctx context.Context, key string, limit int64) (string, error) {
	rc, err := s.blobs.GetObject(ctx, key)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	b, err := io.ReadAll(io.LimitReader(rc, limit))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// GetContent streams an object's bytes. Used by the web UI so the
// browser can fetch content same-origin (agents should prefer PresignGet).
func (s *Store) GetContent(ctx context.Context, contentKey string) (io.ReadCloser, error) {
	return s.blobs.GetObject(ctx, contentKey)
}

// PresignGet returns a time-limited URL the agent can use to fetch content
func (s *Store) PresignGet(ctx context.Context, contentKey string, ttl time.Duration) (string, error) {
	return s.blobs.PresignGetObject(ctx, contentKey, ttl)
}

// --- revisions (immutable per-version history) ---

// Revision is one entry in a document's version history. Old content blobs are
// retained (content-addressed), so any past version stays fetchable.
type Revision struct {
	Version     int       `json:"version"`
	ContentHash string    `json:"content_hash"`
	SizeBytes   int64     `json:"size_bytes"`
	Author      string    `json:"author"`
	CreatedAt   time.Time `json:"created_at"`
}

// DocRevisions lists a document's versions newest-first.
func (s *Store) DocRevisions(ctx context.Context, idOrSlug string) ([]Revision, error) {
	rows, err := s.q(ctx).Query(ctx, `
		select r.version, r.content_hash, r.size_bytes, coalesce(r.author,''), r.created_at
		from document_revisions r join documents d on d.id = r.document_id
		where (d.id::text = $1 or d.slug = $1)
		order by r.version desc`, idOrSlug)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Revision{}
	for rows.Next() {
		var rv Revision
		if err := rows.Scan(&rv.Version, &rv.ContentHash, &rv.SizeBytes, &rv.Author, &rv.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, rv)
	}
	return out, rows.Err()
}

// RevisionContent returns the blob key + content type for a specific past version,
// so its bytes can be streamed from the retained content-addressed store.
func (s *Store) RevisionContent(ctx context.Context, idOrSlug string, version int) (key, contentType string, err error) {
	err = s.q(ctx).QueryRow(ctx, `
		select r.content_key, d.content_type
		from document_revisions r join documents d on d.id = r.document_id
		where (d.id::text = $1 or d.slug = $1) and r.version = $2`, idOrSlug, version).Scan(&key, &contentType)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ErrNotFound
	}
	return key, contentType, err
}

// --- actors (entity registry) ---

type Actor struct {
	Name        string    `json:"name"`
	FirstSeen   time.Time `json:"first_seen"`
	LastSeen    time.Time `json:"last_seen"`
	ActionCount int64     `json:"action_count"`
}

// touchActor records an entity's activity. Best-effort metadata: errors are
// swallowed so attribution bookkeeping never fails the underlying operation.
func (s *Store) touchActor(ctx context.Context, name string) {
	if name == "" {
		return
	}
	_, _ = s.q(ctx).Exec(ctx, `
		insert into actors (workspace, name, action_count) values (current_setting('app.workspace'), $1, 1)
		on conflict (workspace, name) do update set last_seen = now(), action_count = actors.action_count + 1`, name)
}

func (s *Store) ListActors(ctx context.Context) ([]Actor, error) {
	rows, err := s.q(ctx).Query(ctx, `select name, first_seen, last_seen, action_count from actors order by last_seen desc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Actor{}
	for rows.Next() {
		var a Actor
		if err := rows.Scan(&a.Name, &a.FirstSeen, &a.LastSeen, &a.ActionCount); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ActivityItem is one change made by an actor (derived from document_revisions).
type ActivityItem struct {
	DocumentID string    `json:"document_id"`
	Slug       string    `json:"slug"`
	Title      string    `json:"title"`
	Version    int       `json:"version"`
	SizeBytes  int64     `json:"size_bytes"`
	At         time.Time `json:"at"`
}

// ActorActivity returns an entity's most recent document writes — the real
// "last changes by entity", read straight from the immutable revision log.
func (s *Store) ActorActivity(ctx context.Context, name string, limit int) ([]ActivityItem, error) {
	rows, err := s.q(ctx).Query(ctx, `
		select r.document_id, d.slug, d.title, r.version, r.size_bytes, r.created_at
		from document_revisions r join documents d on d.id = r.document_id
		where r.author = $1 order by r.created_at desc limit $2`, name, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ActivityItem{}
	for rows.Next() {
		var a ActivityItem
		if err := rows.Scan(&a.DocumentID, &a.Slug, &a.Title, &a.Version, &a.SizeBytes, &a.At); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func normalizeKinds(raw []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, item := range raw {
		for _, k := range strings.Split(item, ",") {
			k = strings.TrimSpace(k)
			if k != "" && !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	sort.Strings(out)
	return out
}

// kindsHash produces a 64-character hex SHA-256 digest of the canonical
// length-prefixed encoding of the normalized kind strings.
//
// Proof of injectivity:
//  1. Length-prefixed encoding: Each element `k` in the normalized slice is
//     serialized as `fmt.Sprintf("%d:%s;", len(k), k)`. Because the byte length
//     is declared before reading `len(k)` bytes, no character inside `k` (including
//     newlines, colons, semicolons, or null bytes) can alter element boundaries.
//     Therefore, distinct normalized string slices map to distinct byte sequences.
//  2. Cryptographic digest: SHA-256 maps canonical byte sequences to a 256-bit
//     digest. Because SHA-256 is computationally collision-resistant, distinct
//     canonical byte sequences yield distinct 64-hex digests.
//  3. Empty sentinel: The empty set `[]` serializes to 0 bytes (`""`), producing
//     sha256("") = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".
//     Any non-empty set serializes to >= 3 bytes (`1:x;`), producing a distinct digest.
func kindsHash(kinds []string) string {
	var buf bytes.Buffer
	for _, k := range kinds {
		fmt.Fprintf(&buf, "%d:%s;", len(k), k)
	}
	h := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(h[:])
}

type ParsedCursor struct {
	Workspace string
	KindsHash string
	XactID    uint64
	ID        int64
}

func parseCursor(since string) (ParsedCursor, error) {
	if since == "" {
		return ParsedCursor{}, nil
	}
	parts := strings.Split(since, ":")
	if len(parts) != 4 {
		return ParsedCursor{}, ErrBadCursor
	}
	ws := parts[0]
	hash := parts[1]
	xactID, err1 := strconv.ParseUint(parts[2], 10, 64)
	id, err2 := strconv.ParseInt(parts[3], 10, 64)
	if err1 != nil || err2 != nil || id < 0 || ws == "" || len(hash) != 64 {
		return ParsedCursor{}, ErrBadCursor
	}
	return ParsedCursor{
		Workspace: ws,
		KindsHash: hash,
		XactID:    xactID,
		ID:        id,
	}, nil
}

func eventCursor(e Event, kinds []string) string {
	ws := e.Workspace
	if ws == "" {
		ws = "default"
	}
	return fmt.Sprintf("%s:%s:%s:%d", ws, kindsHash(normalizeKinds(kinds)), e.XactID, e.ID)
}

// ListEvents retrieves change events starting after the cursor `since`.
func (s *Store) ListEvents(ctx context.Context, since string, kinds []string, limit int) ([]Event, string, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	reqWS := requestWorkspace(ctx).name
	if reqWS == "" {
		_ = s.q(ctx).QueryRow(ctx, `select current_setting('app.workspace', true)`).Scan(&reqWS)
	}
	if reqWS == "" {
		reqWS = "default"
	}
	normKinds := normalizeKinds(kinds)
	reqHash := kindsHash(normKinds)

	parsed, err := parseCursor(since)
	if err != nil {
		return nil, since, err
	}

	if since != "" {
		if parsed.Workspace != reqWS || parsed.KindsHash != reqHash {
			return nil, since, fmt.Errorf("%w: cursor scope (%s, %s) does not match request scope (%s, %s)",
				ErrCursorScopeMismatch, parsed.Workspace, parsed.KindsHash, reqWS, reqHash)
		}
	}

	query := `select id, ts, workspace, kind, doc_id::text, slug, actor, version, task_id, xact_id::text
		from events
		where (xact_id, id) > ($1::xid8, $2)
		  and xact_id < pg_snapshot_xmin(pg_current_snapshot())`
	args := []any{parsed.XactID, parsed.ID}

	if len(normKinds) > 0 {
		query += ` and kind = any($3)`
		args = append(args, normKinds)
		query += ` order by xact_id asc, id asc limit $4`
		args = append(args, limit)
	} else {
		query += ` order by xact_id asc, id asc limit $3`
		args = append(args, limit)
	}

	rows, err := s.q(ctx).Query(ctx, query, args...)
	if err != nil {
		return nil, since, err
	}
	defer rows.Close()

	events := []Event{}
	nextCursor := since
	if nextCursor == "" {
		nextCursor = fmt.Sprintf("%s:%s:0:0", reqWS, reqHash)
	}
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.TS, &e.Workspace, &e.Kind, &e.DocID, &e.Slug, &e.Actor, &e.Version, &e.TaskID, &e.XactID); err != nil {
			return nil, since, err
		}
		events = append(events, e)
		nextCursor = fmt.Sprintf("%s:%s:%s:%d", reqWS, reqHash, e.XactID, e.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, since, err
	}
	return events, nextCursor, nil
}
