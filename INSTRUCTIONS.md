# Setup & Usage Guide: Multiplayer Leaderboard Engine

This guide provides detailed instructions on setting up the environment, running unit and load tests, accessing Grafana dashboards for performance monitoring, and inspecting logs and telemetry using OpenTelemetry.

---

## 1. Setting Up the Project & Running Tests

### 1.1 Prerequisites
Ensure the following tools are installed on your machine:
- **Docker** and **Docker Compose**
- **Go** 1.22+ (optional locally if running inside containers, but required for local unit testing)
- **k6** (for running load tests)
- **Node.js** (optional, managed via `mise` or system package manager)

---

### 1.2 Running Go Unit and Integration Tests

The repository consists of five Go microservices under the [`services/`](file:///Users/muneale/Documents/projects/mune/Multiplayer-Leaderboard-Engine/services) directory:
- [`player-service`](file:///Users/muneale/Documents/projects/mune/Multiplayer-Leaderboard-Engine/services/player-service) — Player registration and identity management.
- [`score-service`](file:///Users/muneale/Documents/projects/mune/Multiplayer-Leaderboard-Engine/services/score-service) — High-throughput score ingestion path.
- [`ranking-service`](file:///Users/muneale/Documents/projects/mune/Multiplayer-Leaderboard-Engine/services/ranking-service) — Async Kafka consumer updating Redis sorted sets.
- [`query-service`](file:///Users/muneale/Documents/projects/mune/Multiplayer-Leaderboard-Engine/services/query-service) — Read-only leaderboard retrieval API.
- [`snapshot-service`](file:///Users/muneale/Documents/projects/mune/Multiplayer-Leaderboard-Engine/services/snapshot-service) — Background worker for leaderboard backups.

#### Run Tests for a Specific Service
Navigate to the desired service folder and execute `go test`:

```bash
# Run tests for Score Service
cd services/score-service
go test ./...

# Run tests with verbose output
go test -v ./...
```

#### Run Tests Across All Services
To execute tests for all services simultaneously from the root of the project:

```bash
for d in services/*; do
  echo "=== Running tests in $d ==="
  (cd "$d" && go test ./...)
done
```

---

### 1.3 Running Load Tests with k6

Load tests simulate realistic traffic and test system resilience under high concurrency. The load testing scripts are located in [`load-tests/`](file:///Users/muneale/Documents/projects/mune/Multiplayer-Leaderboard-Engine/load-tests).

#### Step 1: Start the Infrastructure & Microservices
Ensure all containerized services are built and running:

```bash
docker compose up --build -d
```

#### Step 2: Seed Initial Data
Before triggering load tests, seed a test player and game via Apache APISIX Gateway:

```bash
# Register a Player
curl -X POST http://localhost:8080/players \
  -H "Content-Type: application/json" \
  -d '{"username": "player1", "region": "EU"}'

# Register a Second Player
curl -X POST http://localhost:8080/players \
  -H "Content-Type: application/json" \
  -d '{"username": "player2", "region": "US"}'
```

#### Step 3: Execute Scenarios
Run any of the three provided k6 test suites:

- **Scenario 1: Score Submission Ingestion Ramp** (Ramps 0 → 500 virtual users submitting scores):
  ```bash
  k6 run load-tests/score-submission.js
  ```
- **Scenario 2: Leaderboard Read Storm** (Simulates 1000 concurrent users querying rankings):
  ```bash
  k6 run load-tests/leaderboard-read.js
  ```
- **Scenario 3: Mixed Workload** (200 concurrent writers + 800 concurrent readers):
  ```bash
  k6 run load-tests/mixed-load.js
  ```

---

## 2. Accessing Grafana Dashboards & Graphics

Grafana provides visual dashboards to monitor real-time system performance, throughput, and latencies across services during standard operation and load tests.

### 2.1 Accessing the Grafana UI
- **URL**: `http://localhost:3000`
- **Default Credentials** (if prompted): User `admin` / Password `admin`

> **Note**: In a complete observability environment, Grafana connects to Prometheus (metrics), Loki (logs), and Tempo (distributed traces) aggregated through the OpenTelemetry Collector.

### 2.2 Key Graphics & Dashboards to Inspect
When viewing dashboards during a load test, pay attention to these key metrics:

| Metric | Description & What to Look For |
| :--- | :--- |
| **`score_submissions_total`** | Rate of accepted score events per second in Score Service. |
| **`leaderboard_query_duration_ms`** | Latency breakdown of Query Service requests (p95 / p99). |
| **`redis_zadd_duration_ms`** | Latency for atomic Redis sorted set operations in Ranking Service. |
| **`kafka_consumer_lag`** | Measure of unprocessed score events buffered in Kafka partitions. |
| **`cache_hit_ratio`** | Percentage of leaderboard read queries served directly from memory. |

---

## 3. Viewing Logs & Data with OpenTelemetry (OTel)

The system utilizes an OpenTelemetry-first observability architecture. Apache APISIX and all Go microservices stream metrics and traces via OTLP (OpenTelemetry Protocol).

### 3.1 OpenTelemetry Architecture Overview
- **OTel Collector Container**: Configured via [`observability/otel-collector-config.yml`](file:///Users/muneale/Documents/projects/mune/Multiplayer-Leaderboard-Engine/observability/otel-collector-config.yml).
- **Receivers**: Listens on port `4317` (gRPC) and `4318` (HTTP).
- **Environment Setting**: Microservices report telemetry to `OTEL_EXPORTER_OTLP_ENDPOINT=otel-collector:4317`.

---

### 3.2 Inspecting Live Telemetry & Collector Output

#### Option A: View OTel Collector Logs
The collector is configured with detailed debug exporting. To view raw trace spans, metrics data, and context metadata being processed in real time:

```bash
docker compose logs -f otel-collector
```

#### Option B: View Microservice Container Logs
Each microservice logs structured JSON events containing contextual trace IDs (`trace_id` and `span_id`) propagated from Apache APISIX:

```bash
# View Score Service ingestion logs
docker compose logs -f score-service

# View Ranking Service processing logs
docker compose logs -f ranking-service

# Stream logs for all services simultaneously
docker compose logs -f
```

---

### 3.3 Inspecting Kafka Streaming Data (Kafka UI)

To inspect raw asynchronous messages (`score-events`) as they pass from Score Service to Ranking Service via Kafka:

- **Kafka UI Endpoint**: `http://localhost:8090`
- Navigate to **Topics** → `score-events` → **Messages** to view live payloads, partition distributions, and consumer group offset positions.

---

### 3.4 Summary of Service URLs

| Service / Tool | Endpoint / URL | Purpose |
| :--- | :--- | :--- |
| **API Gateway (APISIX)** | `http://localhost:8080` | Entry point for all API requests |
| **Grafana Dashboard** | `http://localhost:3000` | UI for metrics, logs, and trace visualizations |
| **Prometheus** | `http://localhost:9090` | Time-series metrics server |
| **Kafka UI** | `http://localhost:8090` | Visual manager for Kafka topics and partitions |
| **OTel Collector (gRPC)** | `http://localhost:4317` | OpenTelemetry gRPC receiver |
| **OTel Collector (HTTP)** | `http://localhost:4318` | OpenTelemetry HTTP receiver |
