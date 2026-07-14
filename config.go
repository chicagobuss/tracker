package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"time"
)

// Config holds runtime settings, all sourced from the environment.
type Config struct {
	DatabaseURL string
	ListenAddrs []string        // one listener started per address (e.g. LAN + ZeroTier + loopback)
	APITokens   map[string]bool // bearer tokens; empty => auth disabled (dev only)

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
}

func loadConfig() Config {
	c := Config{
		DatabaseURL: env("DATABASE_URL", "postgres://tracker:tracker@127.0.0.1:5432/tracker?sslmode=disable"),
		StorageType: env("STORAGE_TYPE", "file"),
		BlobDir:     env("BLOB_DIR", "./data/blobs"),
		BaseURL:     env("BASE_URL", "http://127.0.0.1:8770"),
		// No S3 defaults on purpose: a half-configured S3 backend should fail
		// with "S3_ENDPOINT is not set", not by dialing someone else's host.
		S3Endpoint:  env("S3_ENDPOINT", ""),
		S3AccessKey: env("S3_ACCESS_KEY", ""),
		S3SecretKey: env("S3_SECRET_KEY", ""),
		S3Bucket:    env("S3_BUCKET", ""),
		S3UseSSL:    env("S3_USE_SSL", "false") == "true",
		APITokens:   map[string]bool{},
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
	for _, t := range strings.Split(env("API_TOKENS", ""), ",") {
		if t = strings.TrimSpace(t); t != "" {
			c.APITokens[t] = true
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

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
