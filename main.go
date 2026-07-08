package main

import (
	"context"
	"embed"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

//go:embed web
var webFS embed.FS

//go:embed openapi.yaml
var openapiSpec []byte

func main() {
	cfg := loadConfig()

	ctx := context.Background()
	store, err := openStore(ctx, cfg)
	if err != nil {
		log.Fatalf("startup: %v", err)
	}
	srv := &Server{store: store, cfg: cfg}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.health)
	mux.HandleFunc("GET /version", srv.versionInfo)
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(openapiSpec)
	})

	mux.HandleFunc("POST /docs", srv.auth(srv.createDoc))
	mux.HandleFunc("GET /docs", srv.auth(srv.listDocs))
	mux.HandleFunc("GET /tags", srv.auth(srv.listTags))
	mux.HandleFunc("GET /docs/{id}", srv.auth(srv.getDoc))
	// Trailing-wildcard read so multi-segment folio-file slugs (e.g. myfolio/file.md)
	// resolve too; the single-segment {id} pattern is more specific and still wins.
	mux.HandleFunc("GET /docs/{rest...}", srv.auth(srv.getDoc))
	mux.HandleFunc("GET /docs/{id}/raw", srv.auth(srv.rawDoc))
	mux.HandleFunc("GET /docs/{id}/revisions", srv.auth(srv.listRevisions))
	mux.HandleFunc("GET /docs/{id}/revisions/{version}/raw", srv.auth(srv.rawRevision))
	mux.HandleFunc("PUT /docs/{id}", srv.auth(srv.putDoc))
	mux.HandleFunc("PATCH /docs/{id}", srv.auth(srv.patchDoc))
	// Relabel by full slug too — every folio file's slug is multi-segment.
	mux.HandleFunc("PATCH /docs/{rest...}", srv.auth(srv.patchDoc))

	mux.HandleFunc("POST /docs/{id}/lock", srv.auth(srv.acquireLock))
	mux.HandleFunc("GET /docs/{id}/lock", srv.auth(srv.getLock))
	mux.HandleFunc("DELETE /docs/{id}/lock", srv.auth(srv.releaseLock))

	mux.HandleFunc("POST /tasks", srv.auth(srv.createTask))
	mux.HandleFunc("POST /tasks/claim", srv.auth(srv.claimTask))
	mux.HandleFunc("POST /tasks/{id}/complete", srv.auth(srv.completeTask))

	mux.HandleFunc("GET /actors", srv.auth(srv.listActors))
	mux.HandleFunc("GET /actors/{name}/activity", srv.auth(srv.actorActivity))

	mux.HandleFunc("GET /folios", srv.auth(srv.listFolios))
	mux.HandleFunc("POST /folios", srv.auth(srv.createFolio))
	mux.HandleFunc("GET /folios/{slug}", srv.auth(srv.getFolio))
	mux.HandleFunc("POST /folios/{slug}/files", srv.auth(srv.createFolioFile))
	mux.HandleFunc("GET /folios/{slug}/files/{filename}", srv.auth(srv.getFolioFile))
	mux.HandleFunc("GET /folios/{slug}/files/{filename}/raw", srv.auth(srv.rawFolioFile))

	if cfg.StorageType == "file" {
		blobHandler := http.StripPrefix("/blobs/", http.FileServer(http.Dir(cfg.BlobDir)))
		mux.HandleFunc("GET /blobs/", func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/") {
				http.NotFound(w, r)
				return
			}
			blobHandler.ServeHTTP(w, r)
		})
	}

	// Web UI (unauthenticated static assets; data fetches carry the bearer token).
	webRoot, _ := fs.Sub(webFS, "web")
	usageMD, _ := fs.ReadFile(webRoot, "usage.md")
	serveUsage := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
		_, _ = w.Write(usageMD)
	}
	mux.Handle("GET /static/", http.FileServerFS(webRoot))
	mux.HandleFunc("GET /usage.md", func(w http.ResponseWriter, r *http.Request) { serveUsage(w) })
	// llms.txt convention: a single model-readable doc that orients an agent.
	mux.HandleFunc("GET /llms.txt", func(w http.ResponseWriter, r *http.Request) { serveUsage(w) })
	// Content-negotiate the root: browsers (Accept: text/html) get the JS UI;
	// agents/curl get a readable markdown index instead of an unrenderable shell.
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		wantsHTML := strings.Contains(r.Header.Get("Accept"), "text/html")
		if wantsHTML && r.URL.Query().Get("format") != "md" {
			http.ServeFileFS(w, r, webRoot, "index.html")
			return
		}
		serveUsage(w)
	})

	authState := "DISABLED (no API_TOKENS)"
	if len(cfg.APITokens) > 0 {
		authState = "enabled"
	}

	// One http.Server per listen address (e.g. loopback + LAN + ZeroTier), all
	// sharing the same handler. Bind each up front so a bad/unavailable address
	// fails fast instead of silently dropping an interface.
	servers := make([]*http.Server, 0, len(cfg.ListenAddrs))
	for _, addr := range cfg.ListenAddrs {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("listen on %s: %v", addr, err)
		}
		srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		servers = append(servers, srv)
		log.Printf("tracker %s listening on %s | bucket=%s | auth=%s", appVersion(), addr, cfg.S3Bucket, authState)
		go func() {
			if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
				log.Fatalf("serve %s: %v", addr, err)
			}
		}()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, srv := range servers {
		_ = srv.Shutdown(shutCtx)
	}
	store.db.Close()
}
