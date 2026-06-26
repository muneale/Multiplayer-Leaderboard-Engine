# Multiplayer Leaderboard Engine

> A high-throughput microservices system that ingests player score submissions at scale, ranks them in near real-time using Redis Sorted Sets, and serves leaderboard queries with sub-millisecond latency — fully containerized and load-testable locally.

---

## About This Project

I work with startups and scale-ups to design and build distributed systems — from architecture decisions at the whiteboard to production-grade code. This repository demonstrates how I think about system design: choosing the right patterns for the right problems, being explicit about trade-offs, and building things that can be reasoned about, observed, and extended.

The Leaderboard Engine is a realistic but deliberately scoped example of a high-throughput microservices system. Every decision here — from CQRS to Kafka partition strategy to the choice of Redis Sorted Sets — reflects patterns I apply in real engagements.

> If you're a founder or engineering leader looking for fractional CTO support or hands-on architecture work, feel free to reach out.

---

## Table of Contents

- [System Design Philosophy](#system-design-philosophy)
- [Architecture](#architecture)
- [Services](#services)
- [Data Architecture](#data-architecture)
- [Request Lifecycles](#request-lifecycles)
- [Failure Handling](#failure-handling)
- [Observability](#observability)
- [Production vs This Implementation](#production-vs-this-implementation)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Load Testing](#load-testing)
- [Key Design Decisions](#key-design-decisions)

---

## System Design Philosophy

The system is built around three core principles:

- **Separation of concerns** — writing scores, ranking players, and reading leaderboards are handled by entirely independent services that can be scaled, deployed, and reasoned about independently
- **Async by default** — score submissions return in ~2ms; ranking updates happen asynchronously via Kafka, decoupling ingestion speed from processing speed
- **Right tool for the job** — Redis Sorted Sets handle live rankings, Kafka handles event streaming with durability and replay, PostgreSQL handles persistence and history

All services run locally via Docker Compose. No cloud account required.

---

## Architecture

```
                     ┌──────────────────────────────────────────────────────┐
                     │                    Apache APISIX                      │
                     │             API Gateway  —  port 8080                 │
                     │   routing · rate limiting · auth · tracing headers    │
                     └────────┬──────────────┬───────────────┬──────────────┘
                              │              │               │
              ┌───────────────┼──────────────┼───────────────┼──────────────┐
              ▼               ▼              ▼               ▼              ▼
      ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
      │    Player    │ │    Score     │ │    Query     │ │   Snapshot   │
      │   Service    │ │   Service    │ │   Service    │ │   Service    │
      │  (port 3001) │ │ (port 3002)  │ │ (port 3004)  │ │  (worker)    │
      └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
             │                │                 │                 │
             ▼                ▼                 │                 │
      ┌─────────────┐  ┌─────────────┐          │                 │
      │  PostgreSQL │  │    Kafka    │          │                 │
      └─────────────┘  └──────┬──────┘          │                 │
                              │                 │                 │
                              ▼                 │                 │
                      ┌──────────────┐          │                 │
                      │   Ranking    │          │                 │
                      │   Service    │          │                 │
                      │ (port 3003)  │          │                 │
                      └──────┬───────┘          │                 │
                             │                  │                 │
                             ▼                  ▼                 ▼
                      ┌──────────────────────────────────────────────┐
                      │                    Redis                      │
                      │         Sorted Sets · Player ID Cache         │
                      └──────────────────────────────────────────────┘

                      ┌──────────────────────────────────────────────┐
                      │              Observability Stack              │
                      │   OTel Collector · Tempo · Loki · Grafana    │
                      └──────────────────────────────────────────────┘
```

---

## Services

### API Gateway — Apache APISIX `port 8080`

APISIX is the single entry point for all client traffic. It is a cloud-native API gateway built on Nginx, configured declaratively via its Admin API or `apisix.yml`. Unlike similar services, APISIX supports dynamic plugin loading, built-in OpenTelemetry tracing, and per-route rate limiting without reloads.

**Responsibilities:**

- Route `/players/*` → Player Service
- Route `/scores/*` → Score Service
- Route `/leaderboard/*` → Query Service
- Rate limiting per API key (e.g. max 100 score submissions/sec per consumer)
- Inject `X-Request-ID` tracing header on every request
- Emit traces directly to the OTel Collector via the `opentelemetry` plugin

---

### Player Service `port 3001`

Manages player identity and registration. Intentionally thin — a player registers once, so this service is optimized for correctness rather than throughput.

**Endpoints:**

- `POST /players` — register a new player, returns a UUID
- `GET /players/:id` — fetch player profile

**Storage:** PostgreSQL `players` table. On registration, the player ID is written to a Redis key with a 10-minute TTL so Score Service can validate existence without touching the database on the hot path.

---

### Score Service `port 3002`

The hottest path in the system and the primary target for load testing. Stateless and horizontally scalable — run as many replicas as needed behind APISIX.

**Endpoint:**

- `POST /scores` — accepts `{ player_id, game_id, score, timestamp }`

**Behavior:**

1. Validates that `player_id` exists via Redis cache — not PostgreSQL
2. Publishes a `score.submitted` event to Kafka, partitioned by `game_id`
3. Returns `202 Accepted` immediately — does **not** wait for the leaderboard to update

No database writes happen in this service. This is what allows it to absorb massive bursts. Replicas share no state; the gateway load-balances across them with zero coordination.

---

### Ranking Service `port 3003`

Consumes Kafka events and keeps Redis Sorted Sets current. One consumer group instance per Kafka partition, so multiple games are ranked in parallel without contention between instances.

**Behavior:**

- Consumes `score.submitted` events from Kafka topic `score-events`
- Calls `ZADD leaderboard:{game_id} GT {score} {player_id}` on Redis
- The `GT` flag ensures a player's score only ever moves upward — no compare-and-swap logic needed
- Before applying `ZADD`, deduplicates by `event_id` using a Redis Set to handle Kafka redeliveries safely
- If Redis is temporarily unavailable, Kafka buffers events and Ranking Service replays them on recovery from its last committed offset

---

### Query Service `port 3004`

Read-only. Never writes to anything. Optimized purely for fast leaderboard responses.

**Endpoints:**

- `GET /leaderboard/:game_id?page=1&size=50` — paginated top-N leaderboard
- `GET /leaderboard/:game_id/player/:player_id` — a specific player's rank and score
- `GET /leaderboard/:game_id/around/:player_id` — the players ranked just above and below a given player

**Redis commands powering each endpoint:**

| Endpoint     | Command                              | Complexity   |
| ------------ | ------------------------------------ | ------------ |
| Top N        | `ZREVRANGE ... WITHSCORES`           | O(log N + M) |
| Player rank  | `ZREVRANK`                           | O(log N)     |
| Neighborhood | `ZREVRANK` + `ZREVRANGE` with offset | O(log N)     |

Top-50 results are cached in-process for 1 second. This means 1000 req/sec against the same leaderboard key produces only 1 Redis query per second. The tradeoff is 1 second of staleness — acceptable for a leaderboard.

---

### Snapshot Service — background worker

A scheduled background worker that runs entirely outside the hot path.

**Behavior:**

- Every 5 minutes, reads the full leaderboard from Redis (`ZREVRANGE ... 0 -1 WITHSCORES`)
- Persists it to the `leaderboard_snapshots` table in PostgreSQL as JSONB with a timestamp
- Enables historical queries ("what did the leaderboard look like at 14:35?")
- Acts as a recovery point if Redis loses data

---

## Data Architecture

### Redis — live data

```
# Leaderboard sorted set per game
Key:    leaderboard:{game_id}
Type:   Sorted Set
Member: player_id
Score:  numeric score (float)

# Player existence cache — used by Score Service for fast validation
Key:    player:exists:{player_id}
Type:   String
Value:  "1"
TTL:    10 minutes

# Deduplication set — used by Ranking Service to prevent double-counting
Key:    dedup:score-events
Type:   Set
Member: event_id
TTL:    1 hour
```

### PostgreSQL — persistent data

```sql
players
  id          UUID PRIMARY KEY
  username    VARCHAR(64) NOT NULL
  region      VARCHAR(32)
  created_at  TIMESTAMP DEFAULT now()

games
  id          UUID PRIMARY KEY
  name        VARCHAR(128) NOT NULL
  active      BOOLEAN DEFAULT true

leaderboard_snapshots
  id          UUID PRIMARY KEY
  game_id     UUID REFERENCES games(id)
  captured_at TIMESTAMP DEFAULT now()
  data        JSONB   -- full ranked list at capture time
```

---

## Request Lifecycles

### Score Submission

```
1.  Client          →  POST /scores  →  APISIX (rate limit check, inject X-Request-ID)
2.  APISIX          →  routes to Score Service replica (one of N)
3.  Score Service   →  checks Redis: player:exists:{player_id} → HIT
4.  Score Service   →  publishes to Kafka "score-events", partition = hash(game_id)
5.  Score Service   →  returns 202 Accepted                    [~2–5ms total]
6.  (async) Ranking Service consumes the event from Kafka
7.  Ranking Service →  checks dedup:score-events for event_id  → not seen
8.  Ranking Service →  ZADD leaderboard:{game_id} GT {score} {player_id}
9.  Redis           →  sorted set updated                      [~10–50ms after step 5]
```

### Leaderboard Query

```
1.  Client        →  GET /leaderboard/game-123?page=1  →  APISIX
2.  APISIX        →  routes to Query Service
3.  Query Service →  checks in-process cache            →  MISS (cold start)
4.  Query Service →  ZREVRANGE leaderboard:game-123 0 49 WITHSCORES
5.  Redis         →  returns top 50 with scores         [~0.5ms]
6.  Query Service →  enriches player IDs with usernames (local cache)
7.  Query Service →  stores result in in-process cache for 1 second
8.  Query Service →  returns JSON response              [~5–10ms total]

    Subsequent requests within 1 second → in-process cache HIT → <1ms
```

---

## Failure Handling

Knowing what a system does when things go wrong is as important as knowing what it does when they go right. Here is an honest map of what this implementation covers, and what it does not.

### What is handled

**Ranking Service crash:**
Kafka retains events on disk. When Ranking Service restarts, it replays from its last committed offset. No score events are lost and no manual intervention is needed.

**Redis temporarily unavailable:**
Score Service keeps accepting submissions and publishing to Kafka. Kafka buffers everything. Ranking Service catches up when Redis recovers, replaying the backlog in order.

**Score Service instance crash:**
APISIX detects the unhealthy upstream via health checks and stops routing to it. Other replicas continue serving traffic. Because Score Service is stateless, no state is lost.

**Kafka event redelivery / duplicate processing:**
Ranking Service deduplicates by `event_id` using a Redis Set before applying `ZADD`. The same score event processed twice produces the same leaderboard result — idempotent by design.

**Redis data loss on restart:**
Snapshot Service writes leaderboard state to PostgreSQL every 5 minutes. In a recovery scenario, the leaderboard can be rebuilt from the most recent snapshot plus the Kafka event log for the gap period.

### What is NOT handled (intentional simplifications)

**Score Service → Kafka failure (publish fails):**
If Kafka is down when Score Service tries to publish, the submission is lost. The production solution is the **Transactional Outbox pattern**: write the event to a PostgreSQL `outbox` table atomically with any other writes, then have a relay process forward it to Kafka reliably. This is omitted here to keep the service count manageable.

**Kafka consumer lag growing unbounded:**
If Score Service publishes faster than Ranking Service can consume, lag grows. Production adds consumer lag alerting (via Prometheus `kafka_consumer_lag` metric) and auto-scaling of Ranking Service instances. This is left as a manual concern in this project.

**Redis memory exhaustion:**
Sorted sets grow forever as new players and games are added. Production configures Redis `maxmemory` with an eviction policy, or archives inactive game leaderboards to PostgreSQL. Not configured here.

**Circuit breakers:**
If Player Service is slow, Score Service's Redis cache misses will fall back to a PostgreSQL call that could pile up. A circuit breaker (fail-fast after N consecutive failures) would isolate the fault. Not implemented in this version.

**No authentication or authorization:**
All endpoints are open. Production gates every route at APISIX using JWT validation or an OAuth2 plugin. Intentionally omitted to keep the focus on throughput architecture.

---

## Observability

This project uses the **OpenTelemetry + Grafana OSS stack** — the same combination used by most mid-size production engineering teams today. OTel is the industry standard that has replaced vendor-specific SDKs for instrumentation.

### Stack

```
Services (OTel SDK)
        │
        ▼
OTel Collector          ← single collection point, protocol-agnostic
   ├── → Tempo          ← distributed traces (Jaeger-compatible)
   ├── → Prometheus     ← metrics
   └── → Loki           ← structured logs
              │
              ▼
           Grafana       ← single UI for traces, metrics, and logs
```

APISIX emits traces at the gateway level via its built-in `opentelemetry` plugin — so every request gets a `trace_id` before it even reaches a service.

### What each pillar gives you

**Traces (Tempo):** follow a single score submission across all services — APISIX → Score Service → Kafka → Ranking Service → Redis. When p95 latency spikes during a load test, you open a flame graph and see exactly which hop is slow.

**Metrics (Prometheus):**

| Metric                          | What it shows                          |
| ------------------------------- | -------------------------------------- |
| `score_submissions_total`       | Total events accepted by Score Service |
| `kafka_consumer_lag`            | How far behind Ranking Service is      |
| `redis_zadd_duration_ms`        | Time to update a sorted set entry      |
| `leaderboard_query_duration_ms` | End-to-end Query Service latency       |
| `cache_hit_ratio`               | In-process cache effectiveness         |

**Logs (Loki):** every service emits structured JSON logs. Because OTel injects `trace_id` and `span_id` into every log line, you can jump from a Grafana log panel directly to the trace that produced it. No more grepping across containers.

Open Grafana at `http://localhost:3000` during a k6 run to watch all three pillars in real time from a single dashboard.

---

## Production vs This Implementation

This project uses production-grade **patterns** with deliberately simplified **infrastructure**. The table below is explicit about the gap — because knowing the gap is part of the design.

| Concern          | This project               | Production                                       |
| ---------------- | -------------------------- | ------------------------------------------------ |
| API Gateway      | Single APISIX instance     | APISIX cluster with HA config                    |
| Kafka            | Single broker (KRaft mode) | 3+ broker cluster across AZs                     |
| Redis            | Single instance            | Redis Cluster (sharded) or Redis Sentinel        |
| PostgreSQL       | Single node                | Primary + read replicas + PgBouncer              |
| Auth             | None                       | JWT / OAuth2 validated at APISIX                 |
| TLS              | None                       | TLS everywhere, cert managed by gateway          |
| Outbox pattern   | Not implemented            | Transactional Outbox for reliable Kafka publish  |
| Circuit breakers | Not implemented            | Per-service circuit breakers (e.g. resilience4j) |
| Orchestration    | Docker Compose             | Kubernetes with HPA autoscaling                  |
| Secrets          | Plain env vars             | Vault or Kubernetes Secrets                      |
| CI/CD            | None                       | GitHub Actions → container registry → K8s deploy |

The patterns (CQRS, async ingestion, event-driven ranking, read/write path separation) are identical to what runs in production at scale. The infrastructure around them is the part that changes when you move to a real environment.

---

## Tech Stack

| Component          | Technology                 | Reason                                               |
| ------------------ | -------------------------- | ---------------------------------------------------- |
| API Gateway        | Apache APISIX              | Cloud-native, native OTel plugin, hot-reload routing |
| Services           | Go or Node.js              | Go for raw throughput; Node.js for quick iteration   |
| Message Broker     | Apache Kafka (KRaft)       | Durable, replayable, partition-by-game parallelism   |
| Live Rankings      | Redis Sorted Sets          | O(log N) insert + rank, atomic `GT` updates          |
| Persistent Storage | PostgreSQL                 | Player profiles, game registry, historical snapshots |
| Traces             | OpenTelemetry + Tempo      | Distributed tracing across all services              |
| Metrics            | OpenTelemetry + Prometheus | Req/sec, latency, consumer lag                       |
| Logs               | OpenTelemetry + Loki       | Structured JSON logs correlated by trace_id          |
| Dashboards         | Grafana                    | Single UI for traces, metrics, and logs              |
| Containerization   | Docker + Docker Compose    | One command to run everything locally                |
| Load Testing       | k6                         | Scriptable, outputs p95/p99 latency metrics          |

---

## Project Structure

```
leaderboard-engine/
├── docker-compose.yml
├── gateway/
│   └── apisix/
│       ├── config.yml          # APISIX static config
│       └── apisix.yml          # declarative routes and plugins
├── services/
│   ├── player-service/
│   ├── score-service/
│   ├── ranking-service/
│   ├── query-service/
│   └── snapshot-service/
├── infra/
│   ├── kafka/
│   ├── redis/
│   └── postgres/
│       └── migrations/
├── observability/
│   ├── otel-collector-config.yml
│   ├── prometheus.yml
│   ├── tempo-config.yml
│   ├── loki-config.yml
│   └── grafana/
│       ├── datasources/
│       └── dashboards/
│           ├── leaderboard-overview.json
│           └── load-test-results.json
└── load-tests/
    ├── score-submission.js     # ramp: 0 → 500 users
    ├── leaderboard-read.js     # read storm: 1000 users
    └── mixed-load.js           # 200 writers + 800 readers
```

---

## Getting Started

**Prerequisites:** Docker and Docker Compose installed.

```bash
# Clone the repository
git clone https://github.com/your-username/leaderboard-engine.git
cd leaderboard-engine

# Start all services (gateway, services, kafka, redis, postgres, observability)
docker compose up --build

# Seed a game and two players
curl -X POST http://localhost:8080/players \
  -H "Content-Type: application/json" \
  -d '{"username": "player1", "region": "EU"}'

curl -X POST http://localhost:8080/players \
  -H "Content-Type: application/json" \
  -d '{"username": "player2", "region": "US"}'

# Submit a score
curl -X POST http://localhost:8080/scores \
  -H "Content-Type: application/json" \
  -d '{"player_id": "<uuid>", "game_id": "<uuid>", "score": 4200}'

# Query the leaderboard
curl http://localhost:8080/leaderboard/<game_id>
```

**Service URLs:**

| Service              | URL                          |
| -------------------- | ---------------------------- |
| API Gateway (APISIX) | http://localhost:8080        |
| APISIX Admin API     | http://localhost:9180        |
| Grafana              | http://localhost:3000        |
| Prometheus           | http://localhost:9090        |
| Kafka UI             | http://localhost:8090        |
| OTel Collector       | http://localhost:4317 (gRPC) |

---

## Load Testing

Three k6 scenarios are provided under `load-tests/`. Run them while Grafana is open to watch metrics, traces, and logs update in real time.

**Scenario 1 — Score submission ramp:**
Ramps from 0 to 500 concurrent virtual users over 30 seconds, each submitting 1 score/sec.

```bash
k6 run load-tests/score-submission.js
```

Watch: p95 latency of `POST /scores`, Kafka consumer lag, Redis write throughput.

**Scenario 2 — Leaderboard read storm:**
1000 concurrent users all hitting the same leaderboard endpoint simultaneously.

```bash
k6 run load-tests/leaderboard-read.js
```

Watch: p95 latency dropping under 1ms as the in-process cache warms up, Redis hit rate.

**Scenario 3 — Mixed load:**
200 writers and 800 readers simultaneously. The key question: does read latency degrade when writes are happening?

```bash
k6 run load-tests/mixed-load.js
```

It should not — CQRS keeps the two paths entirely independent. Grafana makes this visible.

---

## Key Design Decisions

**CQRS — Command Query Responsibility Segregation**
Score Service (write path) and Query Service (read path) are completely independent services. Heavy read traffic never competes with write throughput. Each can be scaled separately based on its own load profile.

**Async ingestion via Kafka**
Score Service returns `202 Accepted` without waiting for the leaderboard to update. This decouples client-facing latency from the cost of ranking computation. Kafka provides the durability layer: events survive a Ranking Service restart and are replayed from the last committed offset.

**Redis Sorted Sets as the ranking engine**
`ZADD`, `ZREVRANK`, and `ZREVRANGE` are purpose-built for this problem. The `GT` flag on `ZADD` means a player's score can only ever increase — no read-modify-write loop, no locks, no race conditions.

**Partition-by-game in Kafka**
All score events for a given game are routed to the same Kafka partition and processed by the same Ranking Service instance. Multiple games are ranked in parallel without any cross-instance coordination.

**Idempotent event processing**
Ranking Service deduplicates incoming events by `event_id` before applying any write to Redis. This makes the consumer safe to retry and safe under Kafka's at-least-once delivery guarantee.

**OpenTelemetry from the gateway outward**
Traces start at APISIX and flow through every service via propagated context headers. All three observability signals — traces, metrics, logs — share the same `trace_id`, making it possible to move from a slow metric to the exact trace that caused it to the log lines that explain it.

**Snapshots as a durability safety net**
Redis is the source of truth for live rankings, but not for history. Snapshot Service persists the full leaderboard to PostgreSQL every 5 minutes, enabling both historical queries and a recovery path if Redis state is ever lost.
