package main

import (
	"context"
	"net/http"
	"os"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Tracing is off unless an OTLP endpoint is configured, so a laptop install
// never tries to reach a collector it doesn't have. Everything else (headers,
// protocol details, sampling, extra resource attributes) comes from the
// standard OTEL_* variables the SDK already reads.
func tracingEnabled() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

// setupTracing installs the global tracer provider and returns its shutdown,
// which flushes buffered spans. With tracing off the global provider stays the
// no-op default and every span below costs next to nothing.
func setupTracing(ctx context.Context) (func(context.Context) error, error) {
	if !tracingEnabled() {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	if os.Getenv("OTEL_SERVICE_NAME") == "" {
		os.Setenv("OTEL_SERVICE_NAME", "tracker")
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceVersion(appVersion()),
	))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown, nil
}

// traceHTTP wraps the whole mux; otelhttp names each span after the route the
// mux matched ("GET /docs/{id}"), not the raw path. Health checks are left out:
// a load balancer polls them every few seconds and they would drown the rest.
func traceHTTP(h http.Handler) http.Handler {
	return otelhttp.NewHandler(h, "http", otelhttp.WithFilter(func(r *http.Request) bool {
		return r.URL.Path != "/healthz" && r.URL.Path != "/version"
	}))
}

// annotateSpan adds attributes to the request's span when one is recording.
func annotateSpan(ctx context.Context, attrs ...attribute.KeyValue) {
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attrs...)
	}
}

// tracePool adds a span per Postgres query (SQL text only, never arguments).
func tracePool(cfg *pgxpool.Config) {
	if tracingEnabled() {
		cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	}
}
