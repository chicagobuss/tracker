package main

import (
	"strings"
	"testing"
)

// Config validation is what turns a typo'd .env into a readable startup error
// instead of a mystery timeout, so it's worth pinning down. Needs no database.
func TestConfigValidate(t *testing.T) {
	s3 := func() Config {
		return Config{
			StorageType: "s3",
			S3Endpoint:  "s3.example.com:9000",
			S3AccessKey: "key",
			S3SecretKey: "secret",
			S3Bucket:    "bucket",
		}
	}

	tests := []struct {
		name    string
		cfg     Config
		wantErr string // substring; empty means the config must validate
	}{
		{
			name: "file backend with a blob dir",
			cfg:  Config{StorageType: "file", BlobDir: "./data/blobs"},
		},
		{
			name:    "file backend without a blob dir",
			cfg:     Config{StorageType: "file"},
			wantErr: "BLOB_DIR",
		},
		{
			name: "fully configured s3 backend",
			cfg:  s3(),
		},
		{
			name: "s3 backend missing the endpoint",
			cfg: func() Config {
				c := s3()
				c.S3Endpoint = ""
				return c
			}(),
			wantErr: "S3_ENDPOINT",
		},
		{
			name:    "s3 backend with nothing configured names every missing var",
			cfg:     Config{StorageType: "s3"},
			wantErr: "S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET",
		},
		{
			name:    "unknown backend",
			cfg:     Config{StorageType: "gdrive"},
			wantErr: `must be "file" or "s3"`,
		},
		{
			name:    "empty backend",
			cfg:     Config{},
			wantErr: `must be "file" or "s3"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validate() = nil, want an error mentioning %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("validate() = %q, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}
