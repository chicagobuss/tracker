package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Sentinel errors mapped to HTTP status codes by the handlers.
var (
	ErrNotFound        = errors.New("not found")
	ErrLeaseHeld       = errors.New("lease held by another agent")
	ErrNoLease         = errors.New("caller does not hold a valid lease")
	ErrVersionConflict = errors.New("version conflict")
)

type Document struct {
	ID          string    `json:"id"`
	Slug        string    `json:"slug"`
	Title       string    `json:"title"`
	Kind        string    `json:"kind"`
	ContentKey  string    `json:"content_key,omitempty"`
	ContentHash string    `json:"content_hash,omitempty"`
	ContentType string    `json:"content_type"`
	SizeBytes   int64           `json:"size_bytes"`
	Tags        []string        `json:"tags"`
	Metadata    json.RawMessage `json:"metadata"`
	Version     int             `json:"version"`
	CreatedBy   string    `json:"created_by,omitempty"`
	UpdatedBy   string    `json:"updated_by,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
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

type Store struct {
	db     *pgxpool.Pool
	s3     *minio.Client
	bucket string
}

func openStore(ctx context.Context, cfg Config) (*Store, error) {
	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := db.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	s3, err := minio.New(cfg.S3Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.S3AccessKey, cfg.S3SecretKey, ""),
		Secure: cfg.S3UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("init s3: %w", err)
	}
	exists, err := s3.BucketExists(ctx, cfg.S3Bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		if err := s3.MakeBucket(ctx, cfg.S3Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("create bucket %q: %w", cfg.S3Bucket, err)
		}
	}

	st := &Store{db: db, s3: s3, bucket: cfg.S3Bucket}
	if err := st.migrate(ctx); err != nil {
		return nil, err
	}
	return st, nil
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
		if _, err := s.db.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("apply %s: %w", f, err)
		}
	}
	return nil
}

// docSelect is the column list shared by single/list reads.
const docSelect = `id, slug, title, kind, coalesce(content_key,''), coalesce(content_hash,''),
	content_type, size_bytes, tags, metadata, version, coalesce(created_by,''), coalesce(updated_by,''),
	created_at, updated_at`

func scanDoc(row pgx.Row) (*Document, error) {
	var d Document
	err := row.Scan(&d.ID, &d.Slug, &d.Title, &d.Kind, &d.ContentKey, &d.ContentHash,
		&d.ContentType, &d.SizeBytes, &d.Tags, &d.Metadata, &d.Version, &d.CreatedBy, &d.UpdatedBy,
		&d.CreatedAt, &d.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &d, err
}

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
		row := s.db.QueryRow(ctx, `
			insert into documents (slug, title, kind, tags, metadata, created_by, updated_by)
			values ($1, $2, $3, $4, $5::jsonb, $6, $6)
			returning `+docSelect, slug, title, kind, tags, string(metadata), by)
		doc, err := scanDoc(row)
		if err == nil {
			s.touchActor(ctx, by)
		}
		return doc, err
	}

	// Seed initial content: blob first (content-addressed), then the row + v1 revision.
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	key := "sha256/" + hash
	if _, err := s.s3.PutObject(ctx, s.bucket, key, bytes.NewReader(content), int64(len(content)),
		minio.PutObjectOptions{ContentType: contentType}); err != nil {
		return nil, fmt.Errorf("put blob: %w", err)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		insert into documents (slug, title, kind, tags, metadata, content_key, content_hash,
			content_type, size_bytes, version, created_by, updated_by,
			fts)
		values ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, 1, $10, $10,
			to_tsvector('english', $2 || ' ' || left($11, 100000)))
		returning `+docSelect, slug, title, kind, tags, string(metadata), key, hash,
		contentType, int64(len(content)), by, string(content))
	doc, err := scanDoc(row)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		insert into document_revisions (document_id, version, content_key, content_hash, size_bytes, author)
		values ($1, 1, $2, $3, $4, $5)`, doc.ID, key, hash, int64(len(content)), by); err != nil {
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
	return scanDoc(s.db.QueryRow(ctx, `select `+docSelect+` from documents where `+idClause, idOrSlug))
}

// ListDocuments returns a page of documents matching the filters plus the total
// match count (for pagination). limit defaults to 50 (cap 200) when <= 0.
func (s *Store) ListDocuments(ctx context.Context, q, kind, tag string, limit, offset int) ([]Document, int, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}
	const filter = `where ($1 = '' or fts @@ plainto_tsquery('english', $1))
		  and ($2 = '' or kind = $2)
		  and ($3 = '' or $3 = any(tags))`

	var total int
	if err := s.db.QueryRow(ctx, `select count(*) from documents `+filter, q, kind, tag).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.Query(ctx, `select `+docSelect+` from documents `+filter+`
		order by updated_at desc limit $4 offset $5`, q, kind, tag, limit, offset)
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

// AcquireLease acquires, renews (when leaseToken matches the live holder), or
// steals (when the existing lease has expired) the write-lease on a document.
// Returns ErrLeaseHeld (with the current holder) if a different agent holds a
// live lease.
func (s *Store) AcquireLease(ctx context.Context, docID, owner, reason string, ttl time.Duration, leaseToken string) (*Lease, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Ensure the document exists and serialize concurrent lock attempts.
	var realID string
	err = tx.QueryRow(ctx, `select id from documents where `+idClause+` for update`, docID).Scan(&realID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
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
		// Renew: same holder, valid token.
		_, err = tx.Exec(ctx, `update doc_locks set renewed_at = now(), expires_at = $2, reason = $3 where document_id = $1`, realID, expires, reason)
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
	err := s.db.QueryRow(ctx, `
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
	ct, err := s.db.Exec(ctx, `
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

// WriteContent stores bytes in RustFS (content-addressed) then atomically bumps
// the document version, requiring (a) a live lease held by the caller and
// (b) baseVersion to match the current version (optimistic CAS).
func (s *Store) WriteContent(ctx context.Context, docID, owner, leaseToken string, baseVersion int, contentType string, data []byte) (*Document, error) {
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	key := "sha256/" + hash

	// Upload first; blobs are immutable and content-addressed, so a re-PUT of the
	// same bytes is harmless and a failed DB step only leaves a GC-able orphan.
	_, err := s.s3.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return nil, fmt.Errorf("put blob: %w", err)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var id string
	var version int
	err = tx.QueryRow(ctx, `select id, version from documents where `+idClause+` for update`, docID).Scan(&id, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
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
	// Heartbeat the lease on a successful write.
	_, _ = tx.Exec(ctx, `update doc_locks set renewed_at = now() where document_id = $1`, id)

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.touchActor(ctx, owner)
	return doc, nil
}

// GetContent streams an object's bytes from RustFS. Used by the web UI so the
// browser can fetch content same-origin (agents should prefer PresignGet).
func (s *Store) GetContent(ctx context.Context, contentKey string) (io.ReadCloser, error) {
	return s.s3.GetObject(ctx, s.bucket, contentKey, minio.GetObjectOptions{})
}

// PresignGet returns a time-limited URL the agent can use to fetch content
// bytes straight from RustFS without proxying through this service.
func (s *Store) PresignGet(ctx context.Context, contentKey string, ttl time.Duration) (string, error) {
	u, err := s.s3.PresignedGetObject(ctx, s.bucket, contentKey, ttl, url.Values{})
	if err != nil {
		return "", err
	}
	return u.String(), nil
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
	_, _ = s.db.Exec(ctx, `
		insert into actors (name, action_count) values ($1, 1)
		on conflict (name) do update set last_seen = now(), action_count = actors.action_count + 1`, name)
}

func (s *Store) ListActors(ctx context.Context) ([]Actor, error) {
	rows, err := s.db.Query(ctx, `select name, first_seen, last_seen, action_count from actors order by last_seen desc`)
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
	rows, err := s.db.Query(ctx, `
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
