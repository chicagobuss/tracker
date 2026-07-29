package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestWriteErr_AlreadyExists(t *testing.T) {
	w := httptest.NewRecorder()
	writeErr(w, ErrAlreadyExists)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusConflict)
	}
	if got := w.Body.String(); got != "{\"error\":{\"code\":\"already_exists\",\"message\":\"already exists\"}}\n" {
		t.Errorf("body = %s", got)
	}
}

func TestNormalizeCreateError_UniqueViolation(t *testing.T) {
	err := normalizeCreateError(&pgconn.PgError{Code: "23505"})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("err = %v, want ErrAlreadyExists", err)
	}
}

// Tags handed to a folio-file create used to have nowhere to go: the endpoint
// took only metadata, so callers put them there and they silently stopped being
// searchable — metadata is not matched by the tag filter. folioTags is the merge
// that create now runs, so pin its shape: membership leads, the caller's tags
// follow, and the union dedupes.
func TestFolioTags(t *testing.T) {
	cases := []struct {
		name string
		slug string
		tags []string
		want []string
	}{
		{"no caller tags", "beta", nil, []string{"folio:beta"}},
		{"empty slice", "beta", []string{}, []string{"folio:beta"}},
		{"membership leads", "beta", []string{"course:beta", "status:active"},
			[]string{"folio:beta", "course:beta", "status:active"}},
		{"caller repeats the folio tag", "beta", []string{"folio:beta", "course:beta"},
			[]string{"folio:beta", "course:beta"}},
		{"duplicates collapse", "beta", []string{"course:beta", "course:beta"},
			[]string{"folio:beta", "course:beta"}},
		{"empty tags dropped", "beta", []string{"", "course:beta"},
			[]string{"folio:beta", "course:beta"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := folioTags(tc.slug, tc.tags)
			if len(got) != len(tc.want) {
				t.Fatalf("folioTags(%q, %v) = %v, want %v", tc.slug, tc.tags, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("folioTags(%q, %v) = %v, want %v", tc.slug, tc.tags, got, tc.want)
				}
			}
		})
	}
}

// The folio tag is what folio membership *is*, so a caller must not be able to
// displace it by passing tags of their own.
func TestFolioTags_MembershipSurvivesCallerTags(t *testing.T) {
	got := folioTags("beta", []string{"course:beta", "audience:agents"})
	if got[0] != "folio:beta" {
		t.Fatalf("membership tag = %q, want folio:beta (got %v)", got[0], got)
	}
}
