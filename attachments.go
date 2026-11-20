package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"
	"unicode/utf8"
)

// isTextType reports whether content of this type is text: indexed for search,
// inlined by get_doc, and rendered by the web UI. Everything else is an
// attachment — stored and served byte for byte, found by its title.
func isTextType(contentType string) bool {
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil || mt == "" {
		return contentType == ""
	}
	switch {
	case strings.HasPrefix(mt, "text/"),
		strings.HasSuffix(mt, "+json"), strings.HasSuffix(mt, "+xml"), strings.HasSuffix(mt, "+yaml"):
		return true
	}
	switch mt {
	case "application/json", "application/xml", "application/yaml", "application/x-yaml",
		"application/toml", "application/javascript", "application/x-ndjson", "application/sql":
		return true
	}
	return false
}

// searchText is the part of a document's content that full-text search
// indexes. Postgres text cannot hold invalid UTF-8 or NUL bytes, so binary
// content contributes nothing rather than failing the write.
func searchText(contentType string, data []byte) string {
	if !isTextType(contentType) || !utf8.Valid(data) {
		return ""
	}
	return strings.ReplaceAll(string(data), "\x00", "")
}

// contentArgs reads a write's content from either `content` (text) or
// `content_base64` (any bytes). A nil result means no content was given.
func contentArgs(text, b64 string) ([]byte, error) {
	switch {
	case text != "" && b64 != "":
		return nil, errors.New("pass content or content_base64, not both")
	case b64 != "":
		b, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, fmt.Errorf("content_base64 is not valid base64: %w", err)
		}
		return b, nil
	case text != "":
		return []byte(text), nil
	}
	return nil, nil
}

// uploadType picks a content type for base64 uploads that didn't name one:
// the filename's extension first, then the bytes themselves.
func uploadType(given, name string, data []byte) string {
	if given != "" {
		return given
	}
	if t := mime.TypeByExtension(path.Ext(name)); t != "" {
		return t
	}
	return http.DetectContentType(data)
}

// rawURL is the same-origin path a doc's bytes are served from. It carries the
// workspace because an <img> request can't send an X-Workspace header.
func rawURL(doc *Document, workspace string) string {
	u := "/docs/" + doc.Slug + "/raw"
	if workspace != "" {
		u += "?workspace=" + url.QueryEscape(workspace)
	}
	return u
}

// embedMarkdown is a ready-to-paste reference to an attachment: an image
// embed for images, a link for anything else.
func embedMarkdown(doc *Document, workspace string) string {
	label := doc.Title
	if label == "" {
		label = path.Base(doc.Slug)
	}
	label = strings.NewReplacer("[", "(", "]", ")").Replace(label)
	if strings.HasPrefix(doc.ContentType, "image/") {
		return "![" + label + "](" + rawURL(doc, workspace) + ")"
	}
	return "[" + label + "](" + rawURL(doc, workspace) + ")"
}

// withEmbed adds an attachment's embed snippet to a write response.
func withEmbed(resp map[string]any, doc *Document, workspace string) map[string]any {
	if doc.ContentKey != "" && !isTextType(doc.ContentType) {
		resp["embed"] = embedMarkdown(doc, workspace)
	}
	return resp
}

// setContentHeaders describes served bytes. Content that a browser would run
// (HTML, SVG, XML) is sandboxed: anyone can upload it, and without the sandbox
// it would execute with this origin's access to every workspace.
func setContentHeaders(w http.ResponseWriter, contentType, name string) {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	mt, _, _ := mime.ParseMediaType(contentType)
	switch {
	case mt == "text/html", mt == "image/svg+xml", mt == "application/xhtml+xml",
		strings.HasSuffix(mt, "/xml"), strings.HasSuffix(mt, "+xml"):
		w.Header().Set("Content-Security-Policy", "sandbox")
	}
	if name != "" {
		w.Header().Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": path.Base(name)}))
	}
}
