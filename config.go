package main

import (
	"crypto/rand"
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
		DatabaseURL: env("DATABASE_URL", "postgres://coord:coord@127.0.0.1:5432/coord?sslmode=disable"),
		StorageType: env("STORAGE_TYPE", "file"),
		BlobDir:     env("BLOB_DIR", "./data/blobs"),
		BaseURL:     env("BASE_URL", "http://127.0.0.1:8080"),
		S3Endpoint:  env("S3_ENDPOINT", "192.168.1.100:9000"),
		S3AccessKey: env("S3_ACCESS_KEY", "admin"),
		S3SecretKey: env("S3_SECRET_KEY", "adminpassword"),
		S3Bucket:    env("S3_BUCKET", "coord-docs"),
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
	for _, a := range strings.Split(env("LISTEN_ADDR", "127.0.0.1:8080"), ",") {
		if a = strings.TrimSpace(a); a != "" {
			c.ListenAddrs = append(c.ListenAddrs, a)
		}
	}
	return c
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
