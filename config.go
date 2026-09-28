package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime settings, all sourced from the environment.
type Config struct {
	DatabaseURL string
	ListenAddrs []string // one listener started per address (e.g. LAN + ZeroTier + loopback)
	// Bearer tokens mapped to the workspace they are confined to; empty map =>
	// auth disabled (dev only). A "" value means the token is unscoped and the
	// caller picks its workspace with X-Workspace.
	APITokens map[string]string
	// RequireActor makes X-Actor the sole admission check when bearer auth is
	// disabled. The value is still self-asserted; deploy this only behind a
	// trusted network boundary.
	RequireActor bool

	// DefaultWorkspace is used when neither the token nor X-Workspace names one,
	// which is what keeps pre-workspace agents working unchanged.
	DefaultWorkspace string

	TaskClaimTTL time.Duration // a 'claimed' task older than this is claimable again

	StorageType    string // "s3" or "file"
	BlobDir        string // local directory for "file" storage
	BaseURL        string // external URL to generate self-referential presigned URLs
	BlobSigningKey []byte // HMAC key for expiring local blob URLs (fresh per process)

	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3Bucket    string
	S3UseSSL    bool

	// Blob compaction (compact.go): off | manual | auto.
	Compaction      string
	CompactAfter    time.Duration // a blob is packed once it is this old
	CompactGrace    time.Duration // a packed blob's loose copy is deleted after this
	CompactBatch    int           // blobs per step
	CompactInterval time.Duration // pause between steps when there is nothing to do
}

func loadConfig() Config {
	c := Config{
		DatabaseURL: env("DATABASE_URL", "postgres://tracker:tracker@127.0.0.1:5432/tracker?sslmode=disable"),
		StorageType: env("STORAGE_TYPE", "file"),
		BlobDir:     env("BLOB_DIR", "./data/blobs"),
		BaseURL:     env("BASE_URL", "http://127.0.0.1:8770"),
		// No S3 defaults on purpose: a half-configured S3 backend should fail
		// with "S3_ENDPOINT is not set", not by dialing someone else's host.
		S3Endpoint:   env("S3_ENDPOINT", ""),
		S3AccessKey:  env("S3_ACCESS_KEY", ""),
		S3SecretKey:  env("S3_SECRET_KEY", ""),
		S3Bucket:     env("S3_BUCKET", ""),
		S3UseSSL:     env("S3_USE_SSL", "false") == "true",
		APITokens:    map[string]string{},
		RequireActor: env("REQUIRE_ACTOR", "false") == "true",

		DefaultWorkspace: env("DEFAULT_WORKSPACE", "default"),

		Compaction: env("COMPACTION", "off"),
	}
	c.CompactAfter = durationEnv("COMPACT_AFTER", time.Hour)
	c.CompactGrace = durationEnv("COMPACT_GRACE", 24*time.Hour)
	c.CompactInterval = durationEnv("COMPACT_INTERVAL", time.Minute)
	c.CompactBatch = 200
	if n, err := strconv.Atoi(env("COMPACT_BATCH", "200")); err == nil && n > 0 {
		c.CompactBatch = n
	}
	c.TaskClaimTTL = time.Hour
	if d, err := time.ParseDuration(env("TASK_CLAIM_TTL", "1h")); err == nil && d > 0 {
		c.TaskClaimTTL = d
	}
	// Per-process key: a restart invalidates outstanding local blob URLs, which
	// is fine — they are short-lived (15 min) by design.
	c.BlobSigningKey = make([]byte, 32)
	if _, err := rand.Read(c.BlobSigningKey); err != nil {
		panic("blob signing key: " + err.Error())
	}
	// "tok" grants access with the caller choosing its workspace; "tok:name"
	// confines the token to one. Confining is what makes a workspace a boundary
	// rather than a label, since X-Workspace is caller-supplied.
	for _, t := range strings.Split(env("API_TOKENS", ""), ",") {
		if t = strings.TrimSpace(t); t == "" {
			continue
		}
		tok, ws, _ := strings.Cut(t, ":")
		if tok = strings.TrimSpace(tok); tok != "" {
			c.APITokens[tok] = strings.TrimSpace(ws)
		}
	}
	for _, a := range strings.Split(env("LISTEN_ADDR", "127.0.0.1:8770"), ",") {
		if a = strings.TrimSpace(a); a != "" {
			c.ListenAddrs = append(c.ListenAddrs, a)
		}
	}
	return c
}

// validate rejects a config that can't work, naming the exact env var to fix.
// Misconfiguration should fail at startup with a readable message rather than as
// a connection timeout to a half-configured backend.
func (c Config) validate() error {
	switch c.Compaction {
	case "", "off", "manual", "auto":
	default:
		return fmt.Errorf("COMPACTION must be off, manual or auto, got %q", c.Compaction)
	}
	return c.validateStorage(c.StorageType, c.BlobDir)
}

// validateStorage checks the settings one blob backend needs. It takes the
// backend explicitly because migrate-blobs uses two at once (source and
// destination), either of which may be S3.
func (c Config) validateStorage(storageType, blobDir string) error {
	switch storageType {
	case "file":
		if blobDir == "" {
			return fmt.Errorf("file blob storage requires BLOB_DIR (e.g. ./data/blobs)")
		}
	case "s3":
		var missing []string
		for _, kv := range []struct{ k, v string }{
			{"S3_ENDPOINT", c.S3Endpoint},
			{"S3_ACCESS_KEY", c.S3AccessKey},
			{"S3_SECRET_KEY", c.S3SecretKey},
			{"S3_BUCKET", c.S3Bucket},
		} {
			if kv.v == "" {
				missing = append(missing, kv.k)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("s3 blob storage requires %s (unset)", strings.Join(missing, ", "))
		}
	default:
		return fmt.Errorf("STORAGE_TYPE must be \"file\" or \"s3\", got %q", storageType)
	}
	return nil
}

// compacting reports whether the compactor runs at all.
func (c Config) compacting() bool { return c.Compaction == "manual" || c.Compaction == "auto" }

func durationEnv(k string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(env(k, "")); err == nil && d >= 0 {
		return d
	}
	return def
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
