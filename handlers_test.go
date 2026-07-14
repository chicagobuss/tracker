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
