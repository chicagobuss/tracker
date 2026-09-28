package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The delta codec leans on the library treating a raw-content dictionary with
// id 0 as the implicit dictionary for frames that name none. Pin that down.
func TestDeltaCodecRoundTrip(t *testing.T) {
	base := []byte(strings.Repeat("the quick brown fox jumps over the lazy dog\n", 200))
	next := append(append([]byte{}, base[:4000]...), []byte("an edit in the middle\n")...)
	next = append(next, base[4000:]...)
	frame, err := encodeDelta(next, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(frame) > 200 {
		t.Errorf("delta of a small edit is %d bytes; expected a few dozen", len(frame))
	}
	back, err := decodeDelta(frame, base)
	if err != nil || !bytes.Equal(back, next) {
		t.Fatalf("round trip failed: %v", err)
	}
	if _, err := decodeDelta(frame, []byte("some other base entirely")); err == nil {
		if b, _ := decodeDelta(frame, []byte("some other base entirely")); bytes.Equal(b, next) {
			t.Fatal("decoded correctly against the wrong base")
		}
	}
}

// compactStore is testStore with the packing read path in front of the local
// backend, as openStore builds it.
func compactStore(t *testing.T) (*Store, *LocalBlobStore) {
	t.Helper()
	s := testStore(t)
	local := s.blobs.(*LocalBlobStore)
	s.blobs = newPackedBlobs(local, s.db)
	return s, local
}

func compactCfg(mode string) Config {
	return Config{Compaction: mode, CompactAfter: 0, CompactGrace: time.Hour, CompactBatch: 5000, CompactInterval: time.Minute}
}

// seed creates n single-revision documents plus one document with `edits`
// revisions, each a small edit of the last, and returns every content key with
// its bytes.
func seed(t *testing.T, s *Store, n, edits int) map[string][]byte {
	t.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(1))
	words := strings.Fields("tracker blob pack zstd delta revision lease workspace agent folio compaction keyframe dictionary")
	want := map[string][]byte{}
	para := func(k int) []byte {
		var b bytes.Buffer
		fmt.Fprintf(&b, "# note %d\n\n", k)
		for i := 0; i < 40+rng.Intn(200); i++ {
			b.WriteString(words[rng.Intn(len(words))])
			b.WriteByte(" \n"[rng.Intn(8)/7])
		}
		return b.Bytes()
	}
	for i := 0; i < n; i++ {
		body := para(i)
		d, err := s.CreateDocument(ctx, fmt.Sprintf("seed/%d", i), "seed", "note", nil, nil, body, "text/markdown", "tester")
		if err != nil {
			t.Fatal(err)
		}
		want[d.ContentKey] = body
	}
	body := bytes.Repeat(para(-1), 20)
	d, err := s.CreateDocument(ctx, "chain", "chain", "note", nil, nil, body, "text/markdown", "tester")
	if err != nil {
		t.Fatal(err)
	}
	want[d.ContentKey] = body
	tok := leaseFor(t, s, d.ID, "tester", time.Minute)
	for v := 1; v <= edits; v++ {
		body = append(append([]byte{}, body...), []byte(fmt.Sprintf("\nedit %d\n", v))...)
		d, err = s.WriteContent(ctx, d.ID, "tester", tok, d.Version, "text/markdown", body)
		if err != nil {
			t.Fatal(err)
		}
		want[d.ContentKey] = body
	}
	return want
}

func readAllKeys(t *testing.T, s *Store, want map[string][]byte) {
	t.Helper()
	for k, v := range want {
		if got := readContent(t, s, k); !bytes.Equal(got, v) {
			t.Fatalf("%s: read back %d bytes, want %d", k, len(got), len(v))
		}
	}
}

func looseExists(l *LocalBlobStore, key string) bool {
	_, err := os.Stat(filepath.Join(l.blobDir, key))
	return err == nil
}

func TestCompaction_ManualBudgetPackReclaim(t *testing.T) {
	s, local := compactStore(t)
	ctx := context.Background()
	want := seed(t, s, 600, 40)
	c := newCompactor(s.db, s.blobs.(*packedBlobs), compactCfg("manual"))

	// Manual mode with no budget packs nothing.
	if rep, err := c.step(ctx); err != nil || rep.Packed != 0 {
		t.Fatalf("no budget: packed %d, err %v", rep.Packed, err)
	}
	// A budget of 100 packs exactly 100 and is spent.
	if err := c.control(ctx, "op", nil, 100); err != nil {
		t.Fatal(err)
	}
	rep, err := c.step(ctx)
	if err != nil || rep.Packed != 100 {
		t.Fatalf("budget 100: packed %d, err %v", rep.Packed, err)
	}
	snap, err := c.snapshot(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Budget != 0 || snap.Blobs.Packed != 100 {
		t.Fatalf("after budget: budget %d packed %d", snap.Budget, snap.Blobs.Packed)
	}
	// Paused: nothing moves even with budget.
	paused, resumed := true, false
	_ = c.control(ctx, "op", &paused, 10000)
	if rep, _ := c.step(ctx); rep.Packed != 0 {
		t.Fatalf("paused but packed %d", rep.Packed)
	}
	_ = c.control(ctx, "op", &resumed, 0)
	if _, err := c.step(ctx); err != nil {
		t.Fatal(err)
	}
	snap, _ = c.snapshot(ctx, 0)
	if snap.Blobs.Packed != int64(len(want)) || snap.Blobs.Eligible != 0 {
		t.Fatalf("after full run: packed %d of %d, eligible %d", snap.Blobs.Packed, len(want), snap.Blobs.Eligible)
	}
	if snap.Dict == nil || snap.Codecs["zstd"].Blobs == 0 || snap.Codecs["delta"].Blobs == 0 {
		t.Fatalf("want a trained dictionary and both codecs in use: %+v", snap.Codecs)
	}
	if snap.Bytes.Ratio < 2 {
		t.Errorf("compression ratio %.2f; expected well above 2 on repetitive text", snap.Bytes.Ratio)
	}
	var maxDepth int
	_ = s.db.QueryRow(ctx, `select max(depth) from blob_locations`).Scan(&maxDepth)
	if maxDepth < 2 || maxDepth > maxDeltaDepth {
		t.Fatalf("max delta depth %d; want chains, capped at %d", maxDepth, maxDeltaDepth)
	}
	// Everything reads back through packs, loose copies still present.
	readAllKeys(t, s, want)

	// Within the grace period nothing is reclaimed.
	if rep, _ := c.step(ctx); rep.Reclaimed != 0 {
		t.Fatalf("reclaimed %d inside the grace period", rep.Reclaimed)
	}
	c.cfg.CompactGrace = 0
	for i := 0; i < 5; i++ {
		if _, err := c.step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for k := range want {
		if looseExists(local, k) {
			t.Fatalf("%s: loose copy survived reclaim", k)
		}
	}
	readAllKeys(t, s, want)
}

// A damaged pack must never lose data: reads fall back to the loose copy,
// reclaim refuses to delete it and forgets the bad packed copy, and the next
// step packs the blob again.
func TestCompaction_DamagedPack(t *testing.T) {
	s, local := compactStore(t)
	ctx := context.Background()
	want := seed(t, s, 40, 30)
	c := newCompactor(s.db, s.blobs.(*packedBlobs), compactCfg("auto"))
	if _, err := c.step(ctx); err != nil {
		t.Fatal(err)
	}
	var pack string
	if err := s.db.QueryRow(ctx, `select key from blob_packs limit 1`).Scan(&pack); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(local.blobDir, pack)
	b, _ := os.ReadFile(p)
	for i := 10; i < len(b); i += 97 {
		b[i] ^= 0xff
	}
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	st := s.blobs.(*packedBlobs).stats
	readAllKeys(t, s, want)
	if st.reads[readFallback].Load() == 0 {
		t.Fatal("expected reads to fall back to loose copies")
	}
	c.cfg.CompactGrace = 0
	for i := 0; i < 4; i++ {
		if _, err := c.step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Members the damage missed (tiny deltas can sit between flipped bytes)
	// may stay; every remaining packed copy must decode and verify.
	rows, err := s.db.Query(ctx, `select content_key from blob_locations`)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for rows.Next() {
		var k string
		_ = rows.Scan(&k)
		keys = append(keys, k)
	}
	rows.Close()
	pb := s.blobs.(*packedBlobs)
	for _, k := range keys {
		loc, _ := pb.location(ctx, s.db, k)
		if _, err := pb.decode(ctx, k, loc, 0); err != nil {
			t.Fatalf("%s: packed copy still bad after reclaim: %v", k, err)
		}
	}
	var unreclaimed int
	_ = s.db.QueryRow(ctx, `select count(*) from blob_locations where loose_deleted_at is null`).Scan(&unreclaimed)
	if unreclaimed != 0 {
		t.Fatalf("%d blobs repacked but not reclaimed", unreclaimed)
	}
	readAllKeys(t, s, want)
}

// Writes carry on while compaction runs; blobs newer than COMPACT_AFTER are
// left alone.
func TestCompaction_ConcurrentWritesAndAge(t *testing.T) {
	s, _ := compactStore(t)
	ctx := context.Background()
	want := seed(t, s, 30, 5)
	cfg := compactCfg("auto")
	cfg.CompactAfter = time.Hour
	c := newCompactor(s.db, s.blobs.(*packedBlobs), cfg)
	if rep, _ := c.step(ctx); rep.Packed != 0 {
		t.Fatalf("packed %d blobs younger than COMPACT_AFTER", rep.Packed)
	}
	c.cfg.CompactAfter = 0
	done := make(chan error)
	go func() {
		for i := 0; i < 50; i++ {
			body := []byte(fmt.Sprintf("written during compaction %d", i))
			d, err := s.CreateDocument(ctx, fmt.Sprintf("during/%d", i), "t", "note", nil, nil, body, "text/markdown", "tester")
			if err != nil {
				done <- err
				return
			}
			want[d.ContentKey] = body
		}
		done <- nil
	}()
	if _, err := c.step(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := c.step(ctx); err != nil {
		t.Fatal(err)
	}
	readAllKeys(t, s, want)
}

func TestCompactionAdminAndMetrics(t *testing.T) {
	s, _ := compactStore(t)
	cfg := compactCfg("manual")
	cfg.APITokens = map[string]string{"admin": "", "narrow": "default"}
	cfg.DefaultWorkspace = "default"
	srv := &Server{store: s, cfg: cfg}
	srv.compactor = newCompactor(s.db, s.blobs.(*packedBlobs), cfg)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/compaction", srv.auth(srv.compactionStatus))
	mux.HandleFunc("POST /admin/compaction/{action}", srv.auth(srv.compactionControl))
	mux.HandleFunc("GET /metrics", srv.metrics)

	do := func(method, path, tok string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, nil)
		if tok != "" {
			r.Header.Set("Authorization", "Bearer "+tok)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	if w := do("POST", "/admin/compaction/run?blobs=10", "narrow"); w.Code != http.StatusForbidden {
		t.Fatalf("confined token: %d %s", w.Code, w.Body)
	}
	if w := do("POST", "/admin/compaction/run?blobs=0", "admin"); w.Code != http.StatusBadRequest {
		t.Fatalf("zero budget: %d", w.Code)
	}
	w := do("POST", "/admin/compaction/run?blobs=25", "admin")
	var snap compactionSnapshot
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &snap) != nil || snap.Budget != 25 || snap.Mode != "manual" {
		t.Fatalf("run: %d %s", w.Code, w.Body)
	}
	if w := do("POST", "/admin/compaction/pause", "admin"); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"paused":true`) {
		t.Fatalf("pause: %d %s", w.Code, w.Body)
	}
	if w := do("GET", "/metrics", ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("metrics without a token: %d", w.Code)
	}
	w = do("GET", "/metrics", "narrow")
	for _, line := range []string{"tracker_compaction_paused 1", "tracker_compaction_budget_blobs 25", `tracker_compaction_mode{mode="manual"} 1`,
		`tracker_blob_reads_total{source="pack"}`} {
		if !strings.Contains(w.Body.String(), line) {
			t.Fatalf("metrics missing %q:\n%s", line, w.Body)
		}
	}
	srv.cfg.Compaction = "off"
	if w := do("GET", "/admin/compaction", "admin"); w.Code != http.StatusConflict {
		t.Fatalf("compaction off: %d", w.Code)
	}
}

// /blobs/ now serves both backends through the store, so a packed blob whose
// loose copy is gone is still served, and non-content keys are not.
func TestServeBlob_PackedAndKeyShape(t *testing.T) {
	s, local := compactStore(t)
	ctx := context.Background()
	want := seed(t, s, 5, 3)
	c := newCompactor(s.db, s.blobs.(*packedBlobs), Config{Compaction: "auto", CompactBatch: 100})
	for i := 0; i < 3; i++ {
		if _, err := c.step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	srv := &Server{store: s}
	for k, v := range want {
		if looseExists(local, k) {
			t.Fatalf("%s still loose", k)
		}
		w := httptest.NewRecorder()
		srv.serveBlob(w, httptest.NewRequest("GET", "/blobs/"+k, nil))
		if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), v) {
			t.Fatalf("%s: %d, %d bytes", k, w.Code, w.Body.Len())
		}
	}
	var pack string
	_ = s.db.QueryRow(ctx, `select key from blob_packs limit 1`).Scan(&pack)
	for _, k := range []string{pack, "sha256/../../etc/passwd", "sha256/abc"} {
		w := httptest.NewRecorder()
		srv.serveBlob(w, httptest.NewRequest("GET", "/blobs/"+k, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("%s: served with %d", k, w.Code)
		}
	}
	if _, err := os.Stat(filepath.Join(local.blobDir, pack)); errors.Is(err, fs.ErrNotExist) {
		t.Fatal("pack missing on disk")
	}
}
