# OpenTelemetry Go Examples

> **Note**: These examples focus on key instrumentation patterns. Some imports and helper functions are abbreviated or omitted for clarity. In production code, ensure all necessary imports are included and helper functions are properly defined.

## Example 1: Complete HTTP Service with Traces and Metrics

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "time"

    "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/codes"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
    "go.opentelemetry.io/otel/metric"
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/sdk/resource"
    "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

var (
    tracer          = otel.Tracer("user-service")
    meter           = otel.Meter("user-service")
    requestCounter  metric.Int64Counter
    requestDuration metric.Float64Histogram
)

func main() {
    ctx := context.Background()

    // Initialize OpenTelemetry
    shutdown, err := initTelemetry(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer shutdown(ctx)

    // Initialize metrics
    initMetrics()

    // Create HTTP server with instrumentation
    mux := http.NewServeMux()
    mux.HandleFunc("/api/users", handleUsers)
    mux.HandleFunc("/api/users/", handleUserByID)

    // Wrap with otelhttp for automatic instrumentation
    handler := otelhttp.NewHandler(mux, "user-service")

    log.Println("Starting server on :8080")
    if err := http.ListenAndServe(":8080", handler); err != nil {
        log.Fatal(err)
    }
}

func initTelemetry(ctx context.Context) (func(context.Context) error, error) {
    res, err := resource.New(ctx,
        resource.WithAttributes(
            semconv.ServiceName("user-service"),
            semconv.ServiceVersion("1.0.0"),
            semconv.DeploymentEnvironment("production"),
        ),
    )
    if err != nil {
        return nil, err
    }

    traceExporter, err := otlptracegrpc.New(ctx,
        otlptracegrpc.WithEndpoint("localhost:4317"),
        otlptracegrpc.WithInsecure(),
    )
    if err != nil {
        return nil, err
    }

    tp := trace.NewTracerProvider(
        trace.WithBatcher(traceExporter),
        trace.WithResource(res),
    )

    otel.SetTracerProvider(tp)
    otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
        propagation.TraceContext{},
        propagation.Baggage{},
    ))

    return tp.Shutdown, nil
}

func initMetrics() {
    var err error

    requestCounter, err = meter.Int64Counter(
        "http.server.requests",
        metric.WithDescription("Total HTTP requests"),
        metric.WithUnit("{request}"),
    )
    if err != nil {
        log.Fatal(err)
    }

    requestDuration, err = meter.Float64Histogram(
        "http.server.duration",
        metric.WithDescription("HTTP request duration"),
        metric.WithUnit("ms"),
    )
    if err != nil {
        log.Fatal(err)
    }
}

func handleUsers(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    start := time.Now()

    // Get current span and add attributes
    span := trace.SpanFromContext(ctx)
    span.SetAttributes(
        attribute.String("http.route", "/api/users"),
        attribute.String("http.method", r.Method),
    )

    var statusCode int
    defer func() {
        // Record metrics
        duration := float64(time.Since(start).Milliseconds())
        attrs := metric.WithAttributes(
            attribute.String("http.method", r.Method),
            attribute.String("http.route", "/api/users"),
            attribute.Int("http.status_code", statusCode),
        )

        requestCounter.Add(ctx, 1, attrs)
        requestDuration.Record(ctx, duration, attrs)
    }()

    if r.Method == http.MethodGet {
        users, err := getUsers(ctx)
        if err != nil {
            span.RecordError(err)
            span.SetStatus(codes.Error, "Failed to get users")
            statusCode = http.StatusInternalServerError
            http.Error(w, "Internal Server Error", statusCode)
            return
        }

        statusCode = http.StatusOK
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(statusCode)
        json.NewEncoder(w).Encode(users)
    } else {
        statusCode = http.StatusMethodNotAllowed
        http.Error(w, "Method not allowed", statusCode)
    }
}

func handleUserByID(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    
    // Extract user ID from path
    userID := r.URL.Path[len("/api/users/"):]
    
    ctx, span := tracer.Start(ctx, "handleUserByID")
    defer span.End()

    span.SetAttributes(
        attribute.String("user.id", userID),
        attribute.String("http.route", "/api/users/{id}"),
    )

    user, err := getUserByID(ctx, userID)
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, "User not found")
        http.Error(w, "User not found", http.StatusNotFound)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(user)
}

func getUsers(ctx context.Context) ([]User, error) {
    ctx, span := tracer.Start(ctx, "getUsers")
    defer span.End()

    // Simulate database query
    time.Sleep(50 * time.Millisecond)

    users := []User{
        {ID: "1", Name: "Alice", Email: "alice@example.com"},
        {ID: "2", Name: "Bob", Email: "bob@example.com"},
    }

    span.SetAttributes(attribute.Int("users.count", len(users)))
    return users, nil
}

func getUserByID(ctx context.Context, userID string) (*User, error) {
    ctx, span := tracer.Start(ctx, "getUserByID")
    defer span.End()

    span.SetAttributes(attribute.String("user.id", userID))

    // Simulate database query
    time.Sleep(30 * time.Millisecond)

    if userID == "1" {
        return &User{ID: "1", Name: "Alice", Email: "alice@example.com"}, nil
    }

    return nil, fmt.Errorf("user not found")
}

type User struct {
    ID    string `json:"id"`
    Name  string `json:"name"`
    Email string `json:"email"`
}
```

## Example 2: Microservice with HTTP Client Calls

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"

    "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("order-service")

type OrderService struct {
    inventoryURL string
    paymentURL   string
    httpClient   *http.Client
}

func NewOrderService() *OrderService {
    return &OrderService{
        inventoryURL: "http://inventory-service:8080",
        paymentURL:   "http://payment-service:8080",
        httpClient: &http.Client{
            Transport: otelhttp.NewTransport(http.DefaultTransport),
        },
    }
}

func (s *OrderService) CreateOrder(ctx context.Context, order *Order) error {
    ctx, span := tracer.Start(ctx, "CreateOrder")
    defer span.End()

    span.SetAttributes(
        attribute.String("order.id", order.ID),
        attribute.Int("order.items_count", len(order.Items)),
    )

    // Check inventory
    available, err := s.checkInventory(ctx, order)
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, "Inventory check failed")
        return err
    }

    if !available {
        span.AddEvent("Insufficient inventory")
        return fmt.Errorf("items not available")
    }

    // Process payment
    paymentID, err := s.processPayment(ctx, order)
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, "Payment failed")
        return err
    }

    span.SetAttributes(attribute.String("payment.id", paymentID))

    // Save order (would typically save to database)
    span.AddEvent("Order created successfully")
    return nil
}

func (s *OrderService) checkInventory(ctx context.Context, order *Order) (bool, error) {
    ctx, span := tracer.Start(ctx, "checkInventory",
        trace.WithSpanKind(trace.SpanKindClient),
    )
    defer span.End()

    url := fmt.Sprintf("%s/api/inventory/check", s.inventoryURL)
    span.SetAttributes(
        attribute.String("http.url", url),
        attribute.String("http.method", "POST"),
    )

    // Create request body
    body, _ := json.Marshal(order.Items)
    req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
    if err != nil {
        span.RecordError(err)
        return false, err
    }

    req.Header.Set("Content-Type", "application/json")

    // Make request (context automatically propagated by otelhttp)
    resp, err := s.httpClient.Do(req)
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, "HTTP request failed")
        return false, err
    }
    defer resp.Body.Close()

    span.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))

    if resp.StatusCode != http.StatusOK {
        span.SetStatus(codes.Error, "Inventory check failed")
        return false, fmt.Errorf("inventory service error")
    }

    var result struct {
        Available bool `json:"available"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        span.RecordError(err)
        return false, err
    }

    return result.Available, nil
}

func (s *OrderService) processPayment(ctx context.Context, order *Order) (string, error) {
    ctx, span := tracer.Start(ctx, "processPayment",
        trace.WithSpanKind(trace.SpanKindClient),
    )
    defer span.End()

    url := fmt.Sprintf("%s/api/payment", s.paymentURL)
    span.SetAttributes(
        attribute.String("http.url", url),
        attribute.Float64("payment.amount", order.TotalAmount),
    )

    // Create and send request
    body, _ := json.Marshal(map[string]interface{}{
        "order_id": order.ID,
        "amount":   order.TotalAmount,
    })

    req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
    req.Header.Set("Content-Type", "application/json")

    resp, err := s.httpClient.Do(req)
    if err != nil {
        span.RecordError(err)
        return "", err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return "", fmt.Errorf("payment failed")
    }

    var result struct {
        PaymentID string `json:"payment_id"`
    }
    json.NewDecoder(resp.Body).Decode(&result)

    return result.PaymentID, nil
}

type Order struct {
    ID          string
    Items       []OrderItem
    TotalAmount float64
}

type OrderItem struct {
    ProductID string
    Quantity  int
}
```

## Example 3: Database Repository Pattern

```go
package main

import (
    "context"
    "database/sql"
    "fmt"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/codes"
    "go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("user-repository")

type UserRepository struct {
    db *sql.DB
}

func NewUserRepository(db *sql.DB) *UserRepository {
    return &UserRepository{db: db}
}

func (r *UserRepository) FindByID(ctx context.Context, userID string) (*User, error) {
    ctx, span := tracer.Start(ctx, "UserRepository.FindByID",
        trace.WithSpanKind(trace.SpanKindClient),
        trace.WithAttributes(
            attribute.String("db.system", "postgresql"),
            attribute.String("db.name", "users_db"),
            attribute.String("db.operation", "SELECT"),
        ),
    )
    defer span.End()

    query := "SELECT id, name, email, created_at FROM users WHERE id = $1"
    span.SetAttributes(
        attribute.String("db.statement", query),
        attribute.String("user.id", userID),
    )

    var user User
    err := r.db.QueryRowContext(ctx, query, userID).Scan(
        &user.ID,
        &user.Name,
        &user.Email,
        &user.CreatedAt,
    )

    if err != nil {
        if err == sql.ErrNoRows {
            span.SetStatus(codes.Error, "User not found")
            span.SetAttributes(attribute.Bool("user.found", false))
            return nil, fmt.Errorf("user not found")
        }
        span.RecordError(err)
        span.SetStatus(codes.Error, "Database query failed")
        return nil, err
    }

    span.SetAttributes(attribute.Bool("user.found", true))
    return &user, nil
}

func (r *UserRepository) Create(ctx context.Context, user *User) error {
    ctx, span := tracer.Start(ctx, "UserRepository.Create",
        trace.WithSpanKind(trace.SpanKindClient),
        trace.WithAttributes(
            attribute.String("db.system", "postgresql"),
            attribute.String("db.name", "users_db"),
            attribute.String("db.operation", "INSERT"),
        ),
    )
    defer span.End()

    query := "INSERT INTO users (id, name, email) VALUES ($1, $2, $3)"
    span.SetAttributes(attribute.String("db.statement", query))

    _, err := r.db.ExecContext(ctx, query, user.ID, user.Name, user.Email)
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, "Failed to insert user")
        return err
    }

    span.AddEvent("User created successfully")
    return nil
}

func (r *UserRepository) FindAll(ctx context.Context, limit int) ([]*User, error) {
    ctx, span := tracer.Start(ctx, "UserRepository.FindAll",
        trace.WithSpanKind(trace.SpanKindClient),
        trace.WithAttributes(
            attribute.String("db.system", "postgresql"),
            attribute.String("db.operation", "SELECT"),
        ),
    )
    defer span.End()

    query := "SELECT id, name, email, created_at FROM users LIMIT $1"
    span.SetAttributes(
        attribute.String("db.statement", query),
        attribute.Int("query.limit", limit),
    )

    rows, err := r.db.QueryContext(ctx, query, limit)
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, "Query failed")
        return nil, err
    }
    defer rows.Close()

    var users []*User
    for rows.Next() {
        var user User
        if err := rows.Scan(&user.ID, &user.Name, &user.Email, &user.CreatedAt); err != nil {
            span.RecordError(err)
            return nil, err
        }
        users = append(users, &user)
    }

    span.SetAttributes(attribute.Int("users.count", len(users)))
    return users, nil
}

type User struct {
    ID        string
    Name      string
    Email     string
    CreatedAt time.Time
}
```

## Example 4: Worker Pool with Context Propagation

```go
package main

import (
    "context"
    "fmt"
    "sync"
    "time"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
)

var tracer = otel.Tracer("worker-pool")

type WorkerPool struct {
    workers int
}

func NewWorkerPool(workers int) *WorkerPool {
    return &WorkerPool{workers: workers}
}

func (p *WorkerPool) ProcessJobs(ctx context.Context, jobs []Job) error {
    ctx, span := tracer.Start(ctx, "ProcessJobs")
    defer span.End()

    span.SetAttributes(
        attribute.Int("jobs.count", len(jobs)),
        attribute.Int("workers.count", p.workers),
    )

    jobChan := make(chan Job, len(jobs))
    errChan := make(chan error, len(jobs))
    var wg sync.WaitGroup

    // Start workers
    for i := 0; i < p.workers; i++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            p.worker(ctx, workerID, jobChan, errChan)
        }(i)
    }

    // Send jobs
    for _, job := range jobs {
        jobChan <- job
    }
    close(jobChan)

    // Wait for completion
    wg.Wait()
    close(errChan)

    // Check for errors
    var errors []error
    for err := range errChan {
        if err != nil {
            errors = append(errors, err)
        }
    }

    if len(errors) > 0 {
        span.SetAttributes(attribute.Int("errors.count", len(errors)))
        return fmt.Errorf("encountered %d errors", len(errors))
    }

    span.AddEvent("All jobs completed successfully")
    return nil
}

func (p *WorkerPool) worker(ctx context.Context, workerID int, jobs <-chan Job, errors chan<- error) {
    for job := range jobs {
        // Create span for each job with parent context
        _, span := tracer.Start(ctx, fmt.Sprintf("worker-%d.processJob", workerID))
        span.SetAttributes(
            attribute.Int("worker.id", workerID),
            attribute.String("job.id", job.ID),
        )

        err := p.processJob(ctx, job)
        if err != nil {
            span.RecordError(err)
            errors <- err
        }

        span.End()
    }
}

func (p *WorkerPool) processJob(ctx context.Context, job Job) error {
    ctx, span := tracer.Start(ctx, "processJob")
    defer span.End()

    span.SetAttributes(attribute.String("job.type", job.Type))

    // Simulate work
    time.Sleep(100 * time.Millisecond)

    return nil
}

type Job struct {
    ID   string
    Type string
    Data interface{}
}
```

## Example 5: gRPC Service

```go
package main

import (
    "context"
    "log"
    "net"

    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"

    "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"

    pb "example.com/proto/user" // Your protobuf package
)

var tracer = otel.Tracer("user-grpc-service")

type UserServiceServer struct {
    pb.UnimplementedUserServiceServer
    repo *UserRepository
}

func (s *UserServiceServer) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.User, error) {
    // Context already contains span from otelgrpc interceptor
    span := trace.SpanFromContext(ctx)
    span.SetAttributes(attribute.String("user.id", req.UserId))

    user, err := s.repo.FindByID(ctx, req.UserId)
    if err != nil {
        span.RecordError(err)
        return nil, status.Error(codes.NotFound, "user not found")
    }

    return &pb.User{
        Id:    user.ID,
        Name:  user.Name,
        Email: user.Email,
    }, nil
}

func (s *UserServiceServer) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
    span := trace.SpanFromContext(ctx)
    span.SetAttributes(
        attribute.String("user.name", req.Name),
        attribute.String("user.email", req.Email),
    )

    user := &User{
        ID:    generateID(),
        Name:  req.Name,
        Email: req.Email,
    }

    if err := s.repo.Create(ctx, user); err != nil {
        span.RecordError(err)
        return nil, status.Error(codes.Internal, "failed to create user")
    }

    return &pb.User{
        Id:    user.ID,
        Name:  user.Name,
        Email: user.Email,
    }, nil
}

func main() {
    ctx := context.Background()

    // Initialize telemetry
    shutdown, err := initTelemetry(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer shutdown(ctx)

    // Create gRPC server with OpenTelemetry interceptors
    server := grpc.NewServer(
        grpc.UnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
        grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor()),
    )

    // Register service
    repo := NewUserRepository(nil) // Pass real DB connection
    pb.RegisterUserServiceServer(server, &UserServiceServer{repo: repo})

    // Start server
    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatal(err)
    }

    log.Println("gRPC server listening on :50051")
    if err := server.Serve(lis); err != nil {
        log.Fatal(err)
    }
}
```

These examples demonstrate real-world patterns for instrumenting Go applications with OpenTelemetry, covering HTTP services, microservices, databases, concurrent processing, and gRPC.
