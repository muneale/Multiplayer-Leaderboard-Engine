---
name: opentelemetry-go
description: Expert knowledge for instrumenting Go applications with OpenTelemetry. Use when working with Go code instrumentation, adding observability to Go applications, or when asked about OpenTelemetry in Go, tracing in Go, metrics, spans, distributed tracing in Golang, OTLP exporters, or instrumentation for Go HTTP servers/clients, gRPC, databases, goroutines, context propagation, or any Go application.
---

# OpenTelemetry Go Instrumentation

Expert guidance for instrumenting Go applications with OpenTelemetry using manual instrumentation and SDK configuration.

## Quick Start

### Install Dependencies

```bash
go get go.opentelemetry.io/otel \
  go.opentelemetry.io/otel/sdk \
  go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc \
  go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc
```

### Initialize OpenTelemetry

```go
package main

import (
    "context"
    "log"
    
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

func initTracer() (*sdktrace.TracerProvider, error) {
    exporter, err := otlptracegrpc.New(context.Background(),
        otlptracegrpc.WithEndpoint("localhost:4317"),
        otlptracegrpc.WithInsecure(),
    )
    if err != nil {
        return nil, err
    }
    
    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(exporter),
        sdktrace.WithResource(resource.NewWithAttributes(
            semconv.SchemaURL,
            semconv.ServiceName("my-service"),
            semconv.ServiceVersion("1.0.0"),
        )),
    )
    
    otel.SetTracerProvider(tp)
    return tp, nil
}
```

## Core Instrumentation Patterns

### Creating Spans

```go
import (
    "context"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/codes"
)

func processRequest(ctx context.Context, userID string) error {
    tracer := otel.Tracer("my-service")
    ctx, span := tracer.Start(ctx, "process-request")
    defer span.End()
    
    span.SetAttributes(attribute.String("user.id", userID))
    
    if err := doWork(ctx); err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, err.Error())
        return err
    }
    
    span.SetStatus(codes.Ok, "Success")
    return nil
}
```

### Context Propagation

Always pass `context.Context` through your call chain:

```go
func parentFunction(ctx context.Context) {
    ctx, span := tracer.Start(ctx, "parent")
    defer span.End()
    
    // Context automatically propagates to child
    childFunction(ctx)
}

func childFunction(ctx context.Context) {
    // Child span inherits parent context
    _, span := tracer.Start(ctx, "child")
    defer span.End()
    
    // Do work
}
```

## HTTP Instrumentation

### HTTP Server

```go
import (
    "net/http"
    "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
    // Wrap handler with otelhttp middleware
    handler := http.HandlerFunc(handleRequest)
    wrappedHandler := otelhttp.NewHandler(handler, "my-service")
    
    http.Handle("/", wrappedHandler)
    http.ListenAndServe(":8080", nil)
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
    // Span automatically created by middleware
    ctx := r.Context()
    
    // Add custom attributes
    span := trace.SpanFromContext(ctx)
    span.SetAttributes(attribute.String("custom.key", "value"))
    
    w.Write([]byte("Hello, World!"))
}
```

### HTTP Client

```go
import (
    "net/http"
    "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func makeRequest(ctx context.Context, url string) error {
    client := http.Client{
        Transport: otelhttp.NewTransport(http.DefaultTransport),
    }
    
    req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
    if err != nil {
        return err
    }
    
    resp, err := client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    
    return nil
}
```

## gRPC Instrumentation

### gRPC Server

```go
import (
    "google.golang.org/grpc"
    "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
)

func main() {
    server := grpc.NewServer(
        grpc.UnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
        grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor()),
    )
    
    // Register services
    pb.RegisterMyServiceServer(server, &myService{})
}
```

### gRPC Client

```go
func createClient() (*grpc.ClientConn, error) {
    conn, err := grpc.Dial("localhost:50051",
        grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
        grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()),
    )
    return conn, err
}
```

## Metrics

### Creating Metrics

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/metric"
)

func setupMetrics() {
    meter := otel.Meter("my-service")
    
    // Counter
    counter, _ := meter.Int64Counter(
        "http.requests",
        metric.WithDescription("Total HTTP requests"),
    )
    counter.Add(ctx, 1, metric.WithAttributes(
        attribute.String("http.method", "GET"),
    ))
    
    // Histogram
    histogram, _ := meter.Float64Histogram(
        "http.request.duration",
        metric.WithUnit("ms"),
    )
    histogram.Record(ctx, 150.5, metric.WithAttributes(
        attribute.String("http.method", "GET"),
    ))
    
    // Async Gauge
    gauge, _ := meter.Int64ObservableGauge(
        "process.memory.usage",
        metric.WithUnit("bytes"),
    )
    meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
        var m runtime.MemStats
        runtime.ReadMemStats(&m)
        o.ObserveInt64(gauge, int64(m.Alloc))
        return nil
    }, gauge)
}
```

## Database Instrumentation

### SQL Database

```go
import (
    "database/sql"
    "go.opentelemetry.io/contrib/instrumentation/database/sql/otelsql"
    semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

func openDB() (*sql.DB, error) {
    db, err := otelsql.Open("postgres", "connection-string",
        otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
    )
    if err != nil {
        return nil, err
    }
    
    // Register stats to get database metrics
    otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(
        semconv.DBSystemPostgreSQL,
    ))
    
    return db, nil
}
```

## Goroutine Context Propagation

```go
func processInGoroutine(ctx context.Context) {
    ctx, span := tracer.Start(ctx, "parent-operation")
    defer span.End()
    
    // Pass context to goroutine
    go func(ctx context.Context) {
        _, childSpan := tracer.Start(ctx, "child-operation")
        defer childSpan.End()
        
        // Do work
    }(ctx)
}
```

## Sampling

### Probability Sampler

```go
import (
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

tp := sdktrace.NewTracerProvider(
    sdktrace.WithSampler(sdktrace.ParentBased(
        sdktrace.TraceIDRatioBased(0.1), // Sample 10%
    )),
)
```

## Best Practices

1. **Always pass context** - Use `context.Context` for all function calls
2. **Defer span.End()** - Always close spans with `defer span.End()`
3. **Record errors** - Use `span.RecordError(err)` and `span.SetStatus()`
4. **Resource attributes** - Set service.name and other identifying attributes
5. **Use middleware** - Leverage otelhttp and otelgrpc for automatic instrumentation
6. **Context propagation** - Pass context to goroutines for proper tracing
7. **Batch processing** - Use `WithBatcher()` for better performance
8. **Semantic conventions** - Follow OpenTelemetry semantic conventions

## Advanced Topics

For detailed examples covering advanced HTTP patterns, database instrumentation, microservices, and complex scenarios, see [references/examples.md](references/examples.md).

## Troubleshooting

### Spans not appearing
- Verify exporter endpoint is accessible
- Check `span.End()` is called
- Ensure TracerProvider is set globally

### Context not propagating
- Always pass `context.Context` as first parameter
- Check middleware is properly configured
- Verify context is passed to goroutines

### Performance issues
- Use `WithBatcher()` instead of synchronous export
- Adjust batch size and timeout
- Implement appropriate sampling

## Key Dependencies

- `go.opentelemetry.io/otel` - Core API
- `go.opentelemetry.io/otel/sdk` - SDK implementation
- `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc` - OTLP trace exporter
- `go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc` - OTLP metric exporter
- `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp` - HTTP instrumentation
- `go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc` - gRPC instrumentation

## Resources

- [OpenTelemetry Go Docs](https://opentelemetry.io/docs/languages/go/)
- [OpenTelemetry Go GitHub](https://github.com/open-telemetry/opentelemetry-go)
- [OpenTelemetry Go Contrib](https://github.com/open-telemetry/opentelemetry-go-contrib)
