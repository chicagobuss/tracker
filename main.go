package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

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

	mux.HandleFunc("POST /docs", srv.auth(srv.createDoc))
	mux.HandleFunc("GET /docs", srv.auth(srv.listDocs))
	mux.HandleFunc("GET /docs/{id}", srv.auth(srv.getDoc))
	mux.HandleFunc("PUT /docs/{id}", srv.auth(srv.putDoc))

	mux.HandleFunc("POST /docs/{id}/lock", srv.auth(srv.acquireLock))
	mux.HandleFunc("GET /docs/{id}/lock", srv.auth(srv.getLock))
	mux.HandleFunc("DELETE /docs/{id}/lock", srv.auth(srv.releaseLock))

	mux.HandleFunc("POST /tasks", srv.auth(srv.createTask))
	mux.HandleFunc("POST /tasks/claim", srv.auth(srv.claimTask))
	mux.HandleFunc("POST /tasks/{id}/complete", srv.auth(srv.completeTask))

	httpSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		authState := "DISABLED (no API_TOKENS)"
		if len(cfg.APITokens) > 0 {
			authState = "enabled"
		}
		log.Printf("coord listening on %s | bucket=%s | auth=%s", cfg.ListenAddr, cfg.S3Bucket, authState)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutCtx)
	store.db.Close()
}
