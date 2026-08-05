package main

import (
	"net/http/httptest"
	"testing"
)

// Unit coverage for the two pure helpers only. Each fails against the behaviour
// it replaces, verified by reverting the fix on a copied tree: without the query
// fallback workspaceSelector returns "", and without cutRevisionRaw a revision
// path is swallowed by the plain "/raw" suffix.
//
// The PATCH change and the end-to-end routing are covered by DB-backed handler
// tests in silent_writes_db_test.go, not here. An earlier version of this
// comment claimed the PATCH regression was covered when no PATCH test existed.

func TestWorkspaceSelector(t *testing.T) {
	for _, tc := range []struct {
		name, header, query, want string
	}{
		{"header only", "alpha", "", "alpha"},
		// The bug: ?workspace= was never read, so a caller asking for another
		// workspace got the default one back with a 200.
		{"query only", "", "beta", "beta"},
		{"header wins over query", "alpha", "beta", "alpha"},
		{"neither", "", "", ""},
		{"whitespace is not a selection", "  ", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/docs?workspace="+tc.query, nil)
			if tc.header != "" {
				r.Header.Set("X-Workspace", tc.header)
			}
			if got := workspaceSelector(r); got != tc.want {
				t.Fatalf("workspaceSelector = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCutRevisionRaw(t *testing.T) {
	for _, tc := range []struct {
		rest    string
		base    string
		version int
		ok      bool
	}{
		{"myfolio/file.md/revisions/3/raw", "myfolio/file.md", 3, true},
		{"fleet/alpha-testpane-codex/revisions/12/raw", "fleet/alpha-testpane-codex", 12, true},
		// A slug may itself contain "/revisions/", so the split takes the last one.
		{"a/revisions/b/revisions/2/raw", "a/revisions/b", 2, true},
		// Must not steal the plain content route.
		{"myfolio/file.md/raw", "", 0, false},
		{"myfolio/file.md/revisions", "", 0, false},
		{"myfolio/file.md/revisions/notanumber/raw", "", 0, false},
		{"myfolio/file.md", "", 0, false},
	} {
		t.Run(tc.rest, func(t *testing.T) {
			base, version, ok := cutRevisionRaw(tc.rest)
			if ok != tc.ok || base != tc.base || version != tc.version {
				t.Fatalf("cutRevisionRaw(%q) = (%q, %d, %v), want (%q, %d, %v)",
					tc.rest, base, version, ok, tc.base, tc.version, tc.ok)
			}
		})
	}
}
