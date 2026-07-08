package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// runMigrateBlobs copies every referenced content blob from the currently
// configured backend (STORAGE_TYPE) into another backend, verifying each blob's
// bytes against its content-address. It is non-destructive (source is left
// intact) and idempotent (content-addressed, so re-runs converge). It does NOT
// flip the active backend — it prints the cutover step for you to run when ready.
func runMigrateBlobs(args []string) {
	fs := flag.NewFlagSet("migrate-blobs", flag.ExitOnError)
	to := fs.String("to", "", `destination backend: "file" or "s3" (required)`)
	blobDir := fs.String("blob-dir", "", `destination directory for --to file (default: BLOB_DIR)`)
	dryRun := fs.Bool("dry-run", false, "list what would be copied; write nothing")
	verify := fs.Bool("verify", false, "also re-read each blob from the destination and re-hash it")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `tracker migrate-blobs — copy content blobs to another backend.

Copies every referenced blob from the current backend (STORAGE_TYPE) to --to,
verifying each against its content hash. Non-destructive (source is left intact)
and idempotent. On success it prints the cutover step — it does not switch the
active backend for you.

Usage:
  tracker migrate-blobs --to file [--blob-dir DIR] [--dry-run] [--verify]
  tracker migrate-blobs --to s3   [--dry-run] [--verify]

Flags:
`)
		fs.PrintDefaults()
	}
	fs.Parse(args)

	cfg := loadConfig()
	src := cfg.StorageType
	if src == "" {
		src = "s3"
	}
	dst := *to
	if dst != "file" && dst != "s3" {
		log.Fatalf(`migrate-blobs: --to must be "file" or "s3"`)
	}
	dstDir := *blobDir
	if dstDir == "" {
		dstDir = cfg.BlobDir
	}
	if src == dst && (dst != "file" || dstDir == cfg.BlobDir) {
		log.Fatalf("migrate-blobs: source and destination are the same (%s) — nothing to do", dst)
	}

	ctx := context.Background()
	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer db.Close()
	store := &Store{db: db}

	srcStore, err := buildBlobStore(ctx, cfg, src, cfg.BlobDir)
	if err != nil {
		log.Fatalf("source backend (%s): %v", src, err)
	}
	dstStore, err := buildBlobStore(ctx, cfg, dst, dstDir)
	if err != nil {
		log.Fatalf("destination backend (%s): %v", dst, err)
	}

	refs, err := store.AllBlobRefs(ctx)
	if err != nil {
		log.Fatalf("enumerate blobs: %v", err)
	}

	dstLabel := dst
	if dst == "file" {
		dstLabel = fmt.Sprintf("file (%s)", dstDir)
	}
	fmt.Printf("migrate-blobs: %s → %s\n", src, dstLabel)
	fmt.Printf("  %d blobs referenced\n", len(refs))

	var copied, missing, mismatched int
	var totalBytes int64
	for _, ref := range refs {
		data, err := readBlob(ctx, srcStore, ref.Key)
		if err != nil {
			fmt.Printf("  ✗ %s — cannot read from source: %v\n", ref.Key, err)
			missing++
			continue
		}
		// The key IS the sha256 of the content; verify before trusting it.
		sum := sha256.Sum256(data)
		if "sha256/"+hex.EncodeToString(sum[:]) != ref.Key {
			fmt.Printf("  ✗ %s — source bytes do not match hash; skipping\n", ref.Key)
			mismatched++
			continue
		}
		totalBytes += int64(len(data))
		if *dryRun {
			copied++
			continue
		}
		if err := dstStore.PutObject(ctx, ref.Key, data, ref.ContentType); err != nil {
			log.Fatalf("write %s to destination: %v", ref.Key, err)
		}
		if *verify {
			vdata, err := readBlob(ctx, dstStore, ref.Key)
			if err != nil {
				log.Fatalf("verify %s: re-read failed: %v", ref.Key, err)
			}
			vsum := sha256.Sum256(vdata)
			if "sha256/"+hex.EncodeToString(vsum[:]) != ref.Key {
				log.Fatalf("verify %s: destination bytes do not match hash", ref.Key)
			}
		}
		copied++
	}

	fmt.Println()
	if *dryRun {
		fmt.Printf("dry-run: would copy %d blobs (%s); wrote nothing.\n", copied, humanBytes(totalBytes))
	} else {
		fmt.Printf("migrated %d blobs (%s)", copied, humanBytes(totalBytes))
		if *verify {
			fmt.Printf(", destination re-hashed OK")
		}
		fmt.Println(".")
	}
	if missing > 0 {
		fmt.Printf("warning: %d referenced blob(s) missing/unreadable in source.\n", missing)
	}
	if mismatched > 0 {
		fmt.Printf("warning: %d blob(s) failed hash verification (NOT copied).\n", mismatched)
	}

	if !*dryRun && missing == 0 && mismatched == 0 {
		fmt.Printf("\nCutover when ready:\n")
		if dst == "file" {
			fmt.Printf("  1. set STORAGE_TYPE=file (BLOB_DIR=%s) in .env\n", dstDir)
		} else {
			fmt.Printf("  1. set STORAGE_TYPE=s3 in .env\n")
		}
		fmt.Printf("  2. restart (e.g. make deploy)\n")
		fmt.Printf("The source blobs are left in place, so reverting STORAGE_TYPE rolls back.\n")
	}
}

func readBlob(ctx context.Context, bs BlobStore, key string) ([]byte, error) {
	rc, err := bs.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func humanBytes(n int64) string {
	switch {
	case n < 1<<10:
		return fmt.Sprintf("%d B", n)
	case n < 1<<20:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	}
}
