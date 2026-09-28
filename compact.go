package main

// Blob compaction.
//
// Content blobs are written one object (or file) per blob and never change.
// Most are small, and many are near-copies of the previous revision of the same
// document, so one file each costs several times their size on disk. A
// background compactor re-encodes cold blobs and packs them together through
// the same BlobStore, so it works the same way on files and on S3:
//
//   - Each blob is stored as the smallest of three encodings: its raw bytes;
//     zstd with a dictionary trained on this store's own blobs; or zstd with
//     the document's previous revision as the dictionary (a delta). Delta
//     chains are capped at maxDeltaDepth. Past that a blob is stored standalone
//     again, like a video keyframe, so no read decodes more than
//     maxDeltaDepth deltas.
//   - Packs are content-addressed (packs/<sha256>). Every member is decoded
//     and checked before a pack is written, and the pack is read back and
//     hash-checked before any row points at it. One transaction then records
//     every member in blob_locations.
//   - Reads look in blob_locations first and fall back to the loose blob.
//     Every decoded blob is checked against its sha256 key.
//   - A loose copy is deleted only after COMPACT_GRACE, and only if its packed
//     copy decodes and verifies at that moment.
//
// Control: COMPACTION=off|manual|auto. In manual mode nothing is packed until
// an operator grants a budget (POST /admin/compaction/run?blobs=N). Auto packs
// everything older than COMPACT_AFTER. Pause and budget are persisted.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
)

const (
	maxDeltaDepth  = 16      // a chain never needs more than this many deltas decoded
	maxPackBytes   = 4 << 20 // a pack is flushed once it reaches this size
	maxMemberBytes = 4 << 20 // larger blobs stay loose
	dictMinBlobs   = 500     // below this many blobs a trained dictionary is not worth having
	dictSamples    = 2000
	dictHistory    = 1 << 20 // bytes of sample content the dictionary is built from
	compactLockID  = 7263001 // pg advisory lock: one compactor per database

	// A damaged frame can claim any output size; never allocate more than a
	// member could legitimately need.
	decoderMaxMemory = 4 * maxMemberBytes
)

var contentKeyRe = regexp.MustCompile(`^sha256/[0-9a-f]{64}$`)

// keyMatches reports whether data hashes to the name a key gives it. Content
// keys, packs and dictionaries are all named by the sha256 of their bytes.
func keyMatches(key string, data []byte) bool {
	i := strings.LastIndexByte(key, '/')
	sum := sha256.Sum256(data)
	return i >= 0 && key[i+1:] == hex.EncodeToString(sum[:])
}

type blobLoc struct {
	Pack      string
	Off, Len  int64
	Codec     string // raw | zstd | delta
	Dict      string
	Base      string
	Depth     int
	Raw       int64
	Reclaimed bool
}

// packedBlobs is the BlobStore tracker actually uses: the configured backend,
// plus reads of blobs the compactor has moved into packs.
type packedBlobs struct {
	BlobStore // the backend: file or s3
	db        *pgxpool.Pool
	stats     *compactStats

	mu    sync.Mutex
	decs  map[string]*zstd.Decoder // by dictionary key; "" is plain zstd
	dicts map[string][]byte
}

func newPackedBlobs(backend BlobStore, db *pgxpool.Pool) *packedBlobs {
	return &packedBlobs{BlobStore: backend, db: db, stats: &compactStats{}, decs: map[string]*zstd.Decoder{}, dicts: map[string][]byte{}}
}

// q uses the connection a request already holds (see Store.q). Taking a second
// pooled connection per read could exhaust a small pool while every request
// waits on it.
func (p *packedBlobs) q(ctx context.Context) querier {
	if c, ok := ctx.Value(wsConnKey{}).(*pgxpool.Conn); ok {
		return c
	}
	return p.db
}

func (p *packedBlobs) location(ctx context.Context, q querier, key string) (*blobLoc, error) {
	var l blobLoc
	err := q.QueryRow(ctx, `
		select pack_key, pack_offset, pack_length, codec, coalesce(dict_key, ''), coalesce(base_key, ''),
		       depth, raw_size, loose_deleted_at is not null
		from blob_locations where content_key = $1`, key).
		Scan(&l.Pack, &l.Off, &l.Len, &l.Codec, &l.Dict, &l.Base, &l.Depth, &l.Raw, &l.Reclaimed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &l, nil
}

type bytesReadCloser struct{ *bytes.Reader }

func (bytesReadCloser) Close() error { return nil }

func (p *packedBlobs) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	if !contentKeyRe.MatchString(key) {
		return p.BlobStore.GetObject(ctx, key)
	}
	loc, err := p.location(ctx, p.q(ctx), key)
	if err != nil {
		log.Printf("blob %s: location lookup failed (%v); reading the loose copy", key, err)
	}
	if loc == nil {
		p.stats.reads[readLoose].Add(1)
		return p.BlobStore.GetObject(ctx, key)
	}
	b, err := p.decode(ctx, key, loc, 0)
	if err == nil {
		p.stats.reads[readPack].Add(1)
		return bytesReadCloser{bytes.NewReader(b)}, nil
	}
	log.Printf("blob %s: packed copy unreadable (%v); trying the loose copy", key, err)
	p.stats.reads[readFallback].Add(1)
	p.stats.errors[errRead].Add(1)
	return p.BlobStore.GetObject(ctx, key)
}

func (p *packedBlobs) bytesOf(ctx context.Context, key string) ([]byte, error) {
	rc, err := p.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// decode reads one packed member and returns its original bytes, verified
// against its key. A delta resolves its base the same way, so a base that
// lost its packed copy is still found loose.
func (p *packedBlobs) decode(ctx context.Context, key string, loc *blobLoc, hops int) ([]byte, error) {
	if hops > 4*maxDeltaDepth { // chains are capped when written; this only guards against a cycle
		return nil, fmt.Errorf("delta chain longer than %d", 4*maxDeltaDepth)
	}
	frame, err := p.GetRange(ctx, loc.Pack, loc.Off, loc.Len)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", loc.Pack, err)
	}
	var out []byte
	switch loc.Codec {
	case "raw":
		out = frame
	case "zstd":
		d, err := p.decoder(ctx, loc.Dict)
		if err != nil {
			return nil, err
		}
		if out, err = d.DecodeAll(frame, nil); err != nil {
			return nil, err
		}
	case "delta":
		var base []byte
		if bl, err := p.location(ctx, p.q(ctx), loc.Base); err == nil && bl != nil {
			base, err = p.decode(ctx, loc.Base, bl, hops+1)
			if err != nil {
				base = nil
			}
		}
		if base == nil {
			if base, err = readAllFrom(p.BlobStore.GetObject(ctx, loc.Base)); err != nil || !keyMatches(loc.Base, base) {
				return nil, fmt.Errorf("delta base %s unavailable", loc.Base)
			}
		}
		if out, err = decodeDelta(frame, base); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown codec %q", loc.Codec)
	}
	if !keyMatches(key, out) {
		return nil, errors.New("decoded bytes do not match the key")
	}
	return out, nil
}

func readAllFrom(rc io.ReadCloser, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (p *packedBlobs) dict(ctx context.Context, key string) ([]byte, error) {
	p.mu.Lock()
	d, ok := p.dicts[key]
	p.mu.Unlock()
	if ok {
		return d, nil
	}
	d, err := readAllFrom(p.BlobStore.GetObject(ctx, key))
	if err != nil {
		return nil, fmt.Errorf("dictionary %s: %w", key, err)
	}
	if !keyMatches(key, d) {
		return nil, fmt.Errorf("dictionary %s does not match its key", key)
	}
	p.mu.Lock()
	p.dicts[key] = d
	p.mu.Unlock()
	return d, nil
}

// decoder returns a shared decoder for a dictionary ("" = none). DecodeAll is
// safe for concurrent use.
func (p *packedBlobs) decoder(ctx context.Context, dictKey string) (*zstd.Decoder, error) {
	p.mu.Lock()
	d, ok := p.decs[dictKey]
	p.mu.Unlock()
	if ok {
		return d, nil
	}
	opts := []zstd.DOption{zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(decoderMaxMemory)}
	if dictKey != "" {
		dict, err := p.dict(ctx, dictKey)
		if err != nil {
			return nil, err
		}
		opts = append(opts, zstd.WithDecoderDicts(dict))
	}
	d, err := zstd.NewReader(nil, opts...)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	if prev, ok := p.decs[dictKey]; ok {
		d.Close()
		d = prev
	} else {
		p.decs[dictKey] = d
	}
	p.mu.Unlock()
	return d, nil
}

func decodeDelta(frame, base []byte) ([]byte, error) {
	d, err := zstd.NewReader(nil, zstd.WithDecoderDictRaw(0, base), zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(decoderMaxMemory))
	if err != nil {
		return nil, err
	}
	defer d.Close()
	return d.DecodeAll(frame, nil)
}

func encodeDelta(raw, base []byte) ([]byte, error) {
	e, err := zstd.NewWriter(nil, zstd.WithEncoderDictRaw(0, base), zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	defer e.Close()
	return e.EncodeAll(raw, nil), nil
}

// ---------------------------------------------------------------- stats

const (
	readLoose = iota
	readPack
	readFallback
	numReads
)

const (
	errPack = iota
	errVerify
	errReclaim
	errRead
	numErrs
)

var readNames = [numReads]string{"loose", "pack", "fallback"}
var errNames = [numErrs]string{"pack", "verify", "reclaim", "read"}

type compactStats struct {
	reads     [numReads]atomic.Int64
	errors    [numErrs]atomic.Int64
	packed    sync.Map // codec -> *atomic.Int64
	reclaimed atomic.Int64

	mu   sync.Mutex
	last stepReport
}

func (s *compactStats) addPacked(codec string) {
	v, _ := s.packed.LoadOrStore(codec, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

type stepReport struct {
	At        time.Time `json:"at"`
	Seconds   float64   `json:"seconds"`
	Packed    int       `json:"packed"`
	Packs     int       `json:"packs"`
	BytesIn   int64     `json:"bytes_in"`
	BytesOut  int64     `json:"bytes_out"`
	Reclaimed int       `json:"reclaimed"`
	Skipped   int       `json:"skipped"`
	Error     string    `json:"error,omitempty"`
}

// ---------------------------------------------------------------- the compactor

type compactor struct {
	db    *pgxpool.Pool
	blobs *packedBlobs
	cfg   Config
	wake  chan struct{}

	skip   sync.Map // content keys that cannot be packed (unreadable, corrupt); retried after a restart
	encMu  sync.Mutex
	encs   map[string]*zstd.Encoder // by dictionary key
	status struct {
		sync.Mutex
		at   time.Time
		snap *compactionSnapshot
	}
}

func newCompactor(db *pgxpool.Pool, blobs *packedBlobs, cfg Config) *compactor {
	return &compactor{db: db, blobs: blobs, cfg: cfg, wake: make(chan struct{}, 1), encs: map[string]*zstd.Encoder{}}
}

func (c *compactor) poke() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// run steps until ctx ends: back to back while there is work, otherwise every
// COMPACT_INTERVAL or when an operator pokes it.
func (c *compactor) run(ctx context.Context) {
	for {
		rep, err := c.step(ctx)
		if err != nil {
			log.Printf("compaction: %v", err)
		} else if rep.Packed > 0 || rep.Reclaimed > 0 {
			var did []string
			if rep.Packed > 0 {
				did = append(did, fmt.Sprintf("packed %d blobs into %d pack(s), %d → %d bytes", rep.Packed, rep.Packs, rep.BytesIn, rep.BytesOut))
			}
			if rep.Reclaimed > 0 {
				did = append(did, fmt.Sprintf("deleted %d loose copies", rep.Reclaimed))
			}
			log.Printf("compaction: %s in %.1fs", strings.Join(did, "; "), rep.Seconds)
		}
		wait := c.cfg.CompactInterval
		if err == nil && (rep.Packed > 0 || rep.Reclaimed > 0) {
			wait = time.Second // more work is likely; stay gentle but keep going
		}
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
		case <-time.After(wait):
		}
	}
}

// withConn runs fn on one pooled connection that sees every workspace (the
// '*' branch of the RLS policies), restoring the connection's setting after.
func (c *compactor) withConn(ctx context.Context, fn func(*pgxpool.Conn) error) error {
	conn, err := c.db.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var prev string
	if err := conn.QueryRow(ctx, `select coalesce(current_setting('app.workspace', true), '')`).Scan(&prev); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, `select set_config('app.workspace', '*', false)`); err != nil {
		return err
	}
	defer conn.Exec(context.WithoutCancel(ctx), `select set_config('app.workspace', $1, false)`, prev)
	return fn(conn)
}

type candidate struct {
	key     string
	size    int64
	prevKey string
}

func (c *compactor) step(ctx context.Context) (rep stepReport, err error) {
	t0 := time.Now()
	defer func() {
		rep.At, rep.Seconds = t0, time.Since(t0).Seconds()
		if err != nil {
			rep.Error = err.Error()
		}
		if err != nil || rep.Packed > 0 || rep.Reclaimed > 0 || rep.Skipped > 0 {
			c.blobs.stats.mu.Lock()
			c.blobs.stats.last = rep
			c.blobs.stats.mu.Unlock()
		}
	}()
	err = c.withConn(ctx, func(conn *pgxpool.Conn) error {
		var locked bool
		if err := conn.QueryRow(ctx, `select pg_try_advisory_lock($1)`, compactLockID).Scan(&locked); err != nil {
			return err
		}
		if !locked {
			return nil // another tracker process is compacting this database
		}
		defer conn.Exec(context.WithoutCancel(ctx), `select pg_advisory_unlock($1)`, compactLockID)

		var paused bool
		var budget int64
		if err := conn.QueryRow(ctx, `select paused, budget from compaction_control where id = 1`).Scan(&paused, &budget); err != nil {
			return err
		}
		if paused {
			return nil
		}
		n, err := c.reclaim(ctx, conn)
		rep.Reclaimed = n
		if err != nil {
			return err
		}
		limit := int64(c.cfg.CompactBatch)
		if c.cfg.Compaction == "manual" && budget < limit {
			limit = budget
		}
		if limit <= 0 {
			return nil
		}
		return c.pack(ctx, conn, int(limit), &rep)
	})
	return rep, err
}

func (c *compactor) pack(ctx context.Context, conn *pgxpool.Conn, limit int, rep *stepReport) error {
	dictKey, err := c.ensureDict(ctx, conn)
	if err != nil {
		log.Printf("compaction: no dictionary this step: %v", err)
		dictKey = ""
	}
	// Oldest first, so a document's earlier revisions are packed before the
	// later ones that may use them as delta bases. A key used by several
	// revisions is considered once, in the context of its first use.
	rows, err := conn.Query(ctx, `
		with c as (
			select distinct on (r.content_key) r.content_key, r.document_id, r.version, r.size_bytes, r.created_at
			from document_revisions r
			where r.created_at < now() - make_interval(secs => $1)
			  and r.size_bytes <= $2
			  and not exists (select 1 from blob_locations l where l.content_key = r.content_key)
			order by r.content_key, r.created_at
		)
		select c.content_key, c.size_bytes,
		       coalesce((select p.content_key from document_revisions p
		                 where p.document_id = c.document_id and p.version < c.version
		                 order by p.version desc limit 1), '')
		from c order by c.created_at, c.document_id, c.version
		limit $3`, c.cfg.CompactAfter.Seconds(), maxMemberBytes, limit+500)
	if err != nil {
		return err
	}
	var cands []candidate
	for rows.Next() {
		var cd candidate
		if err := rows.Scan(&cd.key, &cd.size, &cd.prevKey); err != nil {
			rows.Close()
			return err
		}
		if _, bad := c.skip.Load(cd.key); !bad && len(cands) < limit {
			cands = append(cands, cd)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(cands) == 0 {
		return nil
	}

	type member struct {
		key, codec, dict, base string
		depth                  int
		raw                    int64
		frame                  []byte
	}
	batchRaw := map[string][]byte{} // this step's blobs, for deltas within the batch
	batchDepth := map[string]int{}
	var pending []member
	var pendingBytes int

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		var buf bytes.Buffer
		var rawTotal int64
		for _, m := range pending {
			buf.Write(m.frame)
			rawTotal += m.raw
		}
		sum := sha256.Sum256(buf.Bytes())
		packKey := "packs/" + hex.EncodeToString(sum[:])
		if err := c.blobs.PutObject(ctx, packKey, buf.Bytes(), "application/octet-stream"); err != nil {
			c.blobs.stats.errors[errPack].Add(1)
			return fmt.Errorf("write %s: %w", packKey, err)
		}
		back, err := readAllFrom(c.blobs.BlobStore.GetObject(ctx, packKey))
		if err != nil || !bytes.Equal(back, buf.Bytes()) {
			c.blobs.stats.errors[errVerify].Add(1)
			return fmt.Errorf("pack %s did not read back intact", packKey)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `insert into blob_packs (key, bytes, members, raw_bytes) values ($1, $2, $3, $4)
			on conflict (key) do nothing`, packKey, buf.Len(), len(pending), rawTotal); err != nil {
			return err
		}
		var off int64
		for _, m := range pending {
			if _, err := tx.Exec(ctx, `
				insert into blob_locations (content_key, pack_key, pack_offset, pack_length, codec, dict_key, base_key, depth, raw_size)
				values ($1, $2, $3, $4, $5, nullif($6, ''), nullif($7, ''), $8, $9)
				on conflict (content_key) do nothing`,
				m.key, packKey, off, len(m.frame), m.codec, m.dict, m.base, m.depth, m.raw); err != nil {
				return err
			}
			off += int64(len(m.frame))
		}
		if c.cfg.Compaction == "manual" {
			if _, err := tx.Exec(ctx, `update compaction_control set budget = greatest(budget - $1, 0) where id = 1`, len(pending)); err != nil {
				return err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		for _, m := range pending {
			c.blobs.stats.addPacked(m.codec)
		}
		rep.Packs++
		rep.Packed += len(pending)
		rep.BytesIn += rawTotal
		rep.BytesOut += int64(buf.Len())
		pending, pendingBytes = nil, 0
		return nil
	}

	for _, cd := range cands {
		if err := ctx.Err(); err != nil {
			return err
		}
		raw, err := readAllFrom(c.blobs.BlobStore.GetObject(ctx, cd.key))
		if err != nil || !keyMatches(cd.key, raw) {
			// Missing or corrupt loose blob: nothing trustworthy to pack. Leave it
			// alone (reads keep failing or succeeding exactly as before).
			c.skip.Store(cd.key, true)
			c.blobs.stats.errors[errPack].Add(1)
			rep.Skipped++
			log.Printf("compaction: skipping %s: loose copy missing or does not match its key", cd.key)
			continue
		}
		m, err := c.encode(ctx, conn, cd, raw, dictKey, batchRaw, batchDepth)
		if err != nil {
			c.skip.Store(cd.key, true)
			c.blobs.stats.errors[errPack].Add(1)
			rep.Skipped++
			log.Printf("compaction: skipping %s: %v", cd.key, err)
			continue
		}
		batchRaw[cd.key], batchDepth[cd.key] = raw, m.depth
		pending = append(pending, member{cd.key, m.codec, m.dict, m.base, m.depth, int64(len(raw)), m.frame})
		pendingBytes += len(m.frame)
		if pendingBytes >= maxPackBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

type encoded struct {
	codec, dict, base string
	depth             int
	frame             []byte
}

// encode picks the smallest of raw, zstd (with the dictionary when there is
// one) and a delta against the previous revision, and proves the choice
// decodes back to the original before it is used.
func (c *compactor) encode(ctx context.Context, conn *pgxpool.Conn, cd candidate, raw []byte, dictKey string,
	batchRaw map[string][]byte, batchDepth map[string]int) (encoded, error) {
	best := encoded{codec: "raw", frame: raw}
	enc, err := c.encoder(ctx, dictKey)
	if err != nil {
		return encoded{}, err
	}
	if z := enc.EncodeAll(raw, nil); len(z) < len(best.frame) {
		best = encoded{codec: "zstd", dict: dictKey, frame: z}
	}
	if cd.prevKey != "" && cd.prevKey != cd.key {
		base, depth, ok := batchRaw[cd.prevKey], batchDepth[cd.prevKey], false
		if base != nil {
			ok = true
		} else if loc, err := c.blobs.location(ctx, conn, cd.prevKey); err == nil && loc != nil {
			if b, err := c.blobs.decode(ctx, cd.prevKey, loc, 0); err == nil {
				base, depth, ok = b, loc.Depth, true
			}
		}
		if ok && depth+1 <= maxDeltaDepth {
			if d, err := encodeDelta(raw, base); err == nil && len(d) < len(best.frame) {
				best = encoded{codec: "delta", base: cd.prevKey, depth: depth + 1, frame: d}
			}
		}
		if best.codec == "delta" {
			if back, err := decodeDelta(best.frame, base); err != nil || !bytes.Equal(back, raw) {
				return encoded{}, errors.New("delta did not round-trip")
			}
			return best, nil
		}
	}
	if best.codec == "zstd" {
		d, err := c.blobs.decoder(ctx, dictKey)
		if err != nil {
			return encoded{}, err
		}
		if back, err := d.DecodeAll(best.frame, nil); err != nil || !bytes.Equal(back, raw) {
			return encoded{}, errors.New("zstd did not round-trip")
		}
	}
	return best, nil
}

func (c *compactor) encoder(ctx context.Context, dictKey string) (*zstd.Encoder, error) {
	c.encMu.Lock()
	defer c.encMu.Unlock()
	if e, ok := c.encs[dictKey]; ok {
		return e, nil
	}
	opts := []zstd.EOption{zstd.WithEncoderLevel(zstd.SpeedBestCompression), zstd.WithEncoderConcurrency(1)}
	if dictKey != "" {
		d, err := c.blobs.dict(ctx, dictKey)
		if err != nil {
			return nil, err
		}
		opts = append(opts, zstd.WithEncoderDict(d))
	}
	e, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		return nil, err
	}
	c.encs[dictKey] = e
	return e, nil
}

// ensureDict returns the current dictionary, training one from a sample of
// this store's blobs the first time there are enough of them.
func (c *compactor) ensureDict(ctx context.Context, conn *pgxpool.Conn) (string, error) {
	var key string
	err := conn.QueryRow(ctx, `select key from blob_dicts order by created_at desc limit 1`).Scan(&key)
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	var n int
	if err := conn.QueryRow(ctx, `select count(distinct content_key) from document_revisions where size_bytes between 16 and $1`,
		maxMemberBytes).Scan(&n); err != nil {
		return "", err
	}
	if n < dictMinBlobs {
		return "", nil
	}
	rows, err := conn.Query(ctx, `
		select content_key from (select distinct content_key from document_revisions where size_bytes between 16 and $1) t
		order by random() limit $2`, maxMemberBytes, dictSamples)
	if err != nil {
		return "", err
	}
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return "", err
		}
		keys = append(keys, k)
	}
	rows.Close()
	var contents [][]byte
	var history bytes.Buffer
	for _, k := range keys {
		b, err := c.blobs.bytesOf(ctx, k)
		if err != nil || !keyMatches(k, b) {
			continue
		}
		if len(b) > 32<<10 {
			b = b[:32<<10]
		}
		contents = append(contents, b)
		if h := b[:min(len(b), 4<<10)]; history.Len()+len(h) <= dictHistory {
			history.Write(h)
		}
	}
	if len(contents) < dictMinBlobs/2 {
		return "", fmt.Errorf("only %d readable samples", len(contents))
	}
	hsum := sha256.Sum256(history.Bytes())
	id := 32768 + binary.BigEndian.Uint32(hsum[:4])%(1<<31-32768) // outside the ranges zstd reserves
	dict, err := buildDict(id, contents, history.Bytes())
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(dict)
	key = "dicts/" + hex.EncodeToString(sum[:])
	if err := c.blobs.PutObject(ctx, key, dict, "application/octet-stream"); err != nil {
		return "", err
	}
	if _, err := conn.Exec(ctx, `insert into blob_dicts (key, bytes, samples) values ($1, $2, $3) on conflict do nothing`,
		key, len(dict), len(contents)); err != nil {
		return "", err
	}
	log.Printf("compaction: trained dictionary %s (%d bytes) from %d blobs", key, len(dict), len(contents))
	return key, nil
}

func buildDict(id uint32, contents [][]byte, history []byte) (dict []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("building dictionary: %v", r)
		}
	}()
	return zstd.BuildDict(zstd.BuildDictOptions{ID: id, Contents: contents, History: history, Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedDefault})
}

// reclaim deletes loose copies whose packed copy has been in place for
// COMPACT_GRACE and still decodes and verifies right now.
func (c *compactor) reclaim(ctx context.Context, conn *pgxpool.Conn) (int, error) {
	rows, err := conn.Query(ctx, `
		select content_key from blob_locations
		where loose_deleted_at is null and packed_at < now() - make_interval(secs => $1)
		order by packed_at limit $2`, c.cfg.CompactGrace.Seconds(), c.cfg.CompactBatch)
	if err != nil {
		return 0, err
	}
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return 0, err
		}
		keys = append(keys, k)
	}
	rows.Close()
	n := 0
	for _, k := range keys {
		loc, err := c.blobs.location(ctx, conn, k)
		if err != nil || loc == nil {
			continue
		}
		if _, err := c.blobs.decode(ctx, k, loc, 0); err != nil {
			c.blobs.stats.errors[errReclaim].Add(1)
			// The packed copy is bad. If the loose copy is intact, forget the
			// packed one so the blob is compacted again; otherwise leave both
			// alone for an operator.
			if loose, lerr := readAllFrom(c.blobs.BlobStore.GetObject(ctx, k)); lerr == nil && keyMatches(k, loose) {
				_, _ = conn.Exec(ctx, `delete from blob_locations where content_key = $1`, k)
				log.Printf("compaction: packed copy of %s unreadable (%v); dropped it, the loose copy stays", k, err)
			} else {
				log.Printf("compaction: packed copy of %s unreadable (%v) and no intact loose copy", k, err)
			}
			continue
		}
		if err := c.blobs.DeleteObject(ctx, k); err != nil {
			c.blobs.stats.errors[errReclaim].Add(1)
			log.Printf("compaction: delete loose %s: %v", k, err)
			continue
		}
		if _, err := conn.Exec(ctx, `update blob_locations set loose_deleted_at = now() where content_key = $1`, k); err != nil {
			return n, err
		}
		c.blobs.stats.reclaimed.Add(1)
		n++
	}
	return n, nil
}

// ---------------------------------------------------------------- status, control, metrics

type compactionSnapshot struct {
	Mode     string `json:"mode"`
	Paused   bool   `json:"paused"`
	Budget   int64  `json:"budget"`
	Settings struct {
		After    string `json:"after"`
		Grace    string `json:"grace"`
		Batch    int    `json:"batch"`
		Interval string `json:"interval"`
	} `json:"settings"`
	Blobs struct {
		Referenced int64 `json:"referenced"` // distinct content keys any revision uses
		Packed     int64 `json:"packed"`
		Eligible   int64 `json:"eligible"`    // loose, older than COMPACT_AFTER, small enough to pack
		Reclaimed  int64 `json:"reclaimed"`   // packed and loose copy deleted
		ReclaimDue int64 `json:"reclaim_due"` // packed, past COMPACT_GRACE, loose copy not yet deleted
	} `json:"blobs"`
	Bytes struct {
		PackedRaw     int64   `json:"packed_raw"`    // original size of everything packed
		PackedStored  int64   `json:"packed_stored"` // what it takes in packs
		Ratio         float64 `json:"ratio"`
		EligibleRaw   int64   `json:"eligible_raw"`
		ReferencedRaw int64   `json:"referenced_raw"`
	} `json:"bytes"`
	Codecs map[string]codecStat `json:"codecs"`
	Packs  int64                `json:"packs"`
	Dict   *struct {
		Key     string    `json:"key"`
		Bytes   int       `json:"bytes"`
		Samples int       `json:"samples"`
		Created time.Time `json:"created_at"`
	} `json:"dictionary"`
	LastStep *stepReport `json:"last_step,omitempty"`
}

type codecStat struct {
	Blobs  int64 `json:"blobs"`
	Raw    int64 `json:"raw_bytes"`
	Stored int64 `json:"stored_bytes"`
}

// snapshot reads the compactor's state from the database; maxAge lets the
// metrics endpoint reuse a recent one instead of querying on every scrape.
func (c *compactor) snapshot(ctx context.Context, maxAge time.Duration) (*compactionSnapshot, error) {
	c.status.Lock()
	defer c.status.Unlock()
	if c.status.snap != nil && time.Since(c.status.at) < maxAge {
		return c.status.snap, nil
	}
	s := &compactionSnapshot{Mode: c.cfg.Compaction, Codecs: map[string]codecStat{}}
	s.Settings.After, s.Settings.Grace = c.cfg.CompactAfter.String(), c.cfg.CompactGrace.String()
	s.Settings.Batch, s.Settings.Interval = c.cfg.CompactBatch, c.cfg.CompactInterval.String()
	err := c.withConn(ctx, func(conn *pgxpool.Conn) error {
		if err := conn.QueryRow(ctx, `select paused, budget from compaction_control where id = 1`).Scan(&s.Paused, &s.Budget); err != nil {
			return err
		}
		if err := conn.QueryRow(ctx, `
			with r as (select content_key, max(size_bytes) size_bytes, min(created_at) created_at
			           from document_revisions group by content_key)
			select count(*), coalesce(sum(r.size_bytes), 0),
			       count(*) filter (where l.content_key is null and r.created_at < now() - make_interval(secs => $1) and r.size_bytes <= $2),
			       coalesce(sum(r.size_bytes) filter (where l.content_key is null and r.created_at < now() - make_interval(secs => $1) and r.size_bytes <= $2), 0)
			from r left join blob_locations l on l.content_key = r.content_key`,
			c.cfg.CompactAfter.Seconds(), maxMemberBytes).
			Scan(&s.Blobs.Referenced, &s.Bytes.ReferencedRaw, &s.Blobs.Eligible, &s.Bytes.EligibleRaw); err != nil {
			return err
		}
		if err := conn.QueryRow(ctx, `
			select count(*), count(*) filter (where loose_deleted_at is not null),
			       count(*) filter (where loose_deleted_at is null and packed_at < now() - make_interval(secs => $1)),
			       coalesce(sum(raw_size), 0), coalesce(sum(pack_length), 0), (select count(*) from blob_packs)
			from blob_locations`, c.cfg.CompactGrace.Seconds()).
			Scan(&s.Blobs.Packed, &s.Blobs.Reclaimed, &s.Blobs.ReclaimDue, &s.Bytes.PackedRaw, &s.Bytes.PackedStored, &s.Packs); err != nil {
			return err
		}
		rows, err := conn.Query(ctx, `select codec, count(*), sum(raw_size), sum(pack_length) from blob_locations group by codec`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var k string
			var cs codecStat
			if err := rows.Scan(&k, &cs.Blobs, &cs.Raw, &cs.Stored); err != nil {
				rows.Close()
				return err
			}
			s.Codecs[k] = cs
		}
		rows.Close()
		var d struct {
			Key     string    `json:"key"`
			Bytes   int       `json:"bytes"`
			Samples int       `json:"samples"`
			Created time.Time `json:"created_at"`
		}
		err = conn.QueryRow(ctx, `select key, bytes, samples, created_at from blob_dicts order by created_at desc limit 1`).
			Scan(&d.Key, &d.Bytes, &d.Samples, &d.Created)
		if err == nil {
			s.Dict = &d
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if s.Bytes.PackedStored > 0 {
		s.Bytes.Ratio = float64(int(float64(s.Bytes.PackedRaw)/float64(s.Bytes.PackedStored)*100)) / 100
	}
	c.blobs.stats.mu.Lock()
	if !c.blobs.stats.last.At.IsZero() {
		last := c.blobs.stats.last
		s.LastStep = &last
	}
	c.blobs.stats.mu.Unlock()
	c.status.snap, c.status.at = s, time.Now()
	return s, nil
}

func (c *compactor) control(ctx context.Context, by string, paused *bool, addBudget int64) error {
	c.status.Lock()
	c.status.snap = nil
	c.status.Unlock()
	return c.withConn(ctx, func(conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, `
			update compaction_control
			set paused = coalesce($1, paused), budget = budget + $2, updated_at = now(), updated_by = $3
			where id = 1`, paused, addBudget, by)
		return err
	})
}

// requireAdmin admits operators only: compaction is store-wide, so a token
// confined to one workspace may not steer it.
func (s *Server) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	if v, _ := r.Context().Value(reqWSKey{}).(reqWS); v.confined {
		writeError(w, http.StatusForbidden, "forbidden", "compaction control needs a token that is not confined to a workspace", nil)
		return false
	}
	if s.compactor == nil || !s.cfg.compacting() {
		writeError(w, http.StatusConflict, "compaction_off", "compaction is off (set COMPACTION=manual or auto)", nil)
		return false
	}
	return true
}

func (s *Server) compactionStatus(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	snap, err := s.compactor.snapshot(r.Context(), 0)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

// compactionControl handles POST /admin/compaction/{action}:
//
//	run?blobs=N  manual mode: allow N more blobs to be packed (they are packed in
//	             batches of COMPACT_BATCH until the budget is spent)
//	pause        stop packing and reclaiming until resumed (survives restarts)
//	resume
//	stop         drop the remaining budget (manual mode)
func (s *Server) compactionControl(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	by := strings.TrimSpace(r.Header.Get("X-Actor"))
	t, f := true, false
	var err error
	switch r.PathValue("action") {
	case "run":
		if s.cfg.Compaction != "manual" {
			writeError(w, http.StatusConflict, "not_manual", "COMPACTION=auto packs continuously; run applies to manual mode", nil)
			return
		}
		n, perr := strconv.ParseInt(r.URL.Query().Get("blobs"), 10, 64)
		if perr != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "bad_request", "blobs must be a positive integer", nil)
			return
		}
		err = s.compactor.control(r.Context(), by, nil, n)
	case "pause":
		err = s.compactor.control(r.Context(), by, &t, 0)
	case "resume":
		err = s.compactor.control(r.Context(), by, &f, 0)
	case "stop":
		err = s.compactor.withConn(r.Context(), func(conn *pgxpool.Conn) error {
			_, err := conn.Exec(r.Context(), `update compaction_control set budget = 0, updated_at = now(), updated_by = $1 where id = 1`, by)
			return err
		})
	default:
		writeError(w, http.StatusNotFound, "not_found", "unknown action (run, pause, resume, stop)", nil)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error(), nil)
		return
	}
	s.compactor.poke()
	s.compactionStatus(w, r)
}

// metricsOK admits a Prometheus scraper: with API tokens configured, any valid
// bearer token; otherwise open, like the rest of an unauthenticated tracker.
// The endpoint exposes counts and sizes only, never keys or content.
func metricsOK(cfg Config, r *http.Request) bool {
	if len(cfg.APITokens) > 0 {
		return bearerOK(cfg, r)
	}
	return true
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	if !metricsOK(s.cfg, r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var b strings.Builder
	metric := func(name, typ, help string) { fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ) }
	metric("tracker_build_info", "gauge", "Version of the running tracker.")
	fmt.Fprintf(&b, "tracker_build_info{version=%q} 1\n", appVersion())

	st := &compactStats{}
	if pb, ok := s.store.blobs.(*packedBlobs); ok {
		st = pb.stats
	}
	metric("tracker_blob_reads_total", "counter", "Content blob reads by where the bytes came from.")
	for i, n := range readNames {
		fmt.Fprintf(&b, "tracker_blob_reads_total{source=%q} %d\n", n, st.reads[i].Load())
	}
	metric("tracker_compaction_errors_total", "counter", "Compaction failures by stage.")
	for i, n := range errNames {
		fmt.Fprintf(&b, "tracker_compaction_errors_total{stage=%q} %d\n", n, st.errors[i].Load())
	}
	metric("tracker_compaction_packed_total", "counter", "Blobs packed by this process, by chosen encoding.")
	for _, codec := range []string{"raw", "zstd", "delta"} {
		var n int64
		if v, ok := st.packed.Load(codec); ok {
			n = v.(*atomic.Int64).Load()
		}
		fmt.Fprintf(&b, "tracker_compaction_packed_total{codec=%q} %d\n", codec, n)
	}
	metric("tracker_compaction_reclaimed_total", "counter", "Loose copies deleted by this process after their packed copy verified.")
	fmt.Fprintf(&b, "tracker_compaction_reclaimed_total %d\n", st.reclaimed.Load())

	if s.compactor != nil && s.cfg.compacting() {
		if snap, err := s.compactor.snapshot(r.Context(), 30*time.Second); err == nil {
			metric("tracker_compaction_mode", "gauge", "Configured compaction mode (1 for the active one).")
			for _, m := range []string{"manual", "auto"} {
				fmt.Fprintf(&b, "tracker_compaction_mode{mode=%q} %d\n", m, btoi(snap.Mode == m))
			}
			metric("tracker_compaction_paused", "gauge", "1 while an operator has paused compaction.")
			fmt.Fprintf(&b, "tracker_compaction_paused %d\n", btoi(snap.Paused))
			metric("tracker_compaction_budget_blobs", "gauge", "Blobs still allowed to be packed in manual mode.")
			fmt.Fprintf(&b, "tracker_compaction_budget_blobs %d\n", snap.Budget)
			metric("tracker_compaction_blobs", "gauge", "Content blobs by compaction state.")
			for _, kv := range []struct {
				k string
				v int64
			}{{"referenced", snap.Blobs.Referenced}, {"packed", snap.Blobs.Packed}, {"eligible", snap.Blobs.Eligible},
				{"reclaimed", snap.Blobs.Reclaimed}, {"reclaim_due", snap.Blobs.ReclaimDue}} {
				fmt.Fprintf(&b, "tracker_compaction_blobs{state=%q} %d\n", kv.k, kv.v)
			}
			metric("tracker_compaction_bytes", "gauge", "Bytes: original size of packed blobs, their size in packs, and what is still eligible.")
			fmt.Fprintf(&b, "tracker_compaction_bytes{kind=\"packed_raw\"} %d\n", snap.Bytes.PackedRaw)
			fmt.Fprintf(&b, "tracker_compaction_bytes{kind=\"packed_stored\"} %d\n", snap.Bytes.PackedStored)
			fmt.Fprintf(&b, "tracker_compaction_bytes{kind=\"eligible_raw\"} %d\n", snap.Bytes.EligibleRaw)
			fmt.Fprintf(&b, "tracker_compaction_bytes{kind=\"referenced_raw\"} %d\n", snap.Bytes.ReferencedRaw)
			metric("tracker_compaction_packs", "gauge", "Pack objects written.")
			fmt.Fprintf(&b, "tracker_compaction_packs %d\n", snap.Packs)
			if snap.LastStep != nil {
				metric("tracker_compaction_last_step_timestamp_seconds", "gauge", "When the last step that did work finished.")
				fmt.Fprintf(&b, "tracker_compaction_last_step_timestamp_seconds %d\n", snap.LastStep.At.Unix())
				metric("tracker_compaction_last_step_seconds", "gauge", "How long the last step that did work took.")
				fmt.Fprintf(&b, "tracker_compaction_last_step_seconds %.3f\n", snap.LastStep.Seconds)
			}
		} else {
			log.Printf("metrics: compaction snapshot: %v", err)
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = io.WriteString(w, b.String())
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
