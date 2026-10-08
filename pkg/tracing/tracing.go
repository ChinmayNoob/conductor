// Package tracing sets up OpenTelemetry tracing and carries trace context
// across the gaps no RPC spans: a task waits in Postgres between being
// submitted and being dispatched, so its W3C traceparent is stored on the
// row, and the dispatch and run spans continue the submitter's trace.
//
// Tracing is off unless OTEL_EXPORTER_OTLP_ENDPOINT (or
// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) is set; the standard OTEL_* variables
// configure the exporter, sampler and service name.
package tracing

import (
	"context"
	"os"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Tracer creates Conductor's spans.
var Tracer = otel.Tracer("github.com/ChinmayNoob/conductor")

var (
	once     sync.Once
	shutdown = func(context.Context) error { return nil }
)

// Setup installs W3C trace-context propagation and, if an OTLP endpoint is
// configured, a tracer provider that exports to it. In dev mode every
// component calls it; the first call wins. It returns a function that
// flushes and stops the exporter.
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	var err error
	once.Do(func() {
		otel.SetTextMapPropagator(propagation.TraceContext{})
		if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
			return
		}
		exp, e := otlptracegrpc.New(ctx)
		if e != nil {
			err = e
			return
		}
		var res *resource.Resource
		res, err = resource.New(ctx,
			resource.WithAttributes(attribute.String("service.name", service)),
			resource.WithFromEnv(), // OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES win
		)
		if err != nil {
			return
		}
		tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
		otel.SetTracerProvider(tp)
		shutdown = tp.Shutdown
	})
	return shutdown, err
}

// TraceParent returns the W3C traceparent of ctx's span, or "" if there is
// none.
func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier["traceparent"]
}

// WithTraceParent returns ctx continuing the trace in traceparent. An empty
// or invalid traceparent leaves ctx as it is.
func WithTraceParent(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}

// TraceID returns the trace ID in a traceparent ("00-<trace>-<span>-<flags>"),
// or "".
func TraceID(traceparent string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || len(parts[1]) != 32 {
		return ""
	}
	return parts[1]
}

// Start starts a span; see trace.Tracer.Start.
func Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return Tracer.Start(ctx, name, opts...)
}
