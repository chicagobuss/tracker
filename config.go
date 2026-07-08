package main

import (
	"os"
	"strings"
)

// Config holds runtime settings, all sourced from the environment.
type Config struct {
	DatabaseURL string
	ListenAddrs []string        // one listener started per address (e.g. LAN + ZeroTier + loopback)
	APITokens   map[string]bool // bearer tokens; empty => auth disabled (dev only)

	StorageType string // "s3" or "file"
	BlobDir     string // local directory for "file" storage
	BaseURL     string // external URL to generate self-referential presigned URLs

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
