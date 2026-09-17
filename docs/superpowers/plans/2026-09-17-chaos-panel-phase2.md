# Chaos Panel Phase 2 (Breadth) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cover every interaction type from the spec with at least one fault: a synchronous gRPC dependency guarded by a circuit breaker (payment), a second async stage (courier), a columnar analytics store (ClickHouse), network faults (netem latency/loss), application-level error injection, and metric-driven degradation on the scheme.

**Architecture:** New services plug into the existing patterns: `common::Edge` metrics on every call, liveness-only `/healthz`, one image with all binaries, node ids equal to container names, `topology.json` as the single source of truth. The chaos controller gains two fault mechanisms besides Docker stop/pause: `docker exec tc netem` inside a target container (through docker-socket-proxy with `EXEC=1`) and a `PUT /ops/fault` endpoint every Rust service exposes for injected error rate/latency. State derivation gets a second input: edges whose live error ratio or p95 latency crosses a threshold degrade their dependent node.

**Tech Stack:** unchanged (Rust 1.98, axum 0.8, tonic 0.14, rdkafka 0.39 cmake-build, Svelte 5) plus `clickhouse/clickhouse-server:25.3` over its HTTP interface via reqwest, `iproute2` in the runtime image, `cap_add: NET_ADMIN` on Rust service containers, `EXEC=1` on the socket proxy.

**Spec:** `docs/superpowers/specs/2026-09-17-chaos-panel-design.md` (sections 2, 3.2, 4.1, 4.2, 11 phase 2)

## Global Constraints

- Node ids in `topology.json` equal Docker container names; the controller only touches listed containers.
- Every service: `GET /healthz` (liveness only), `GET /metrics`, `PUT/DELETE /ops/fault` (new, from `common`).
- Every inter-component call records `edge_requests_total{src,dst,proto,outcome}` and `edge_request_duration_seconds`.
- Order statuses, monotonic: `accepted < paid < cooking < cooked < delivering < delivered`; `delivered` terminal.
- Kafka topics: `orders.created` (order-svc → kitchen), `orders.kitchen-events` (kitchen → order-svc, courier, analytics), `orders.delivery-events` (courier → order-svc, analytics).
- Fault kinds: `stop`, `pause`, `latency` (netem delay 800ms ±200ms), `loss` (netem loss 30%), `app-error` (injected 50% errors). Which node offers which is declared in `topology.json`.
- Faults: one active at a time, TTL default 60 s, cooldown 15 s, per-IP 120 s, `ADMIN_TOKEN` bypasses (unchanged).
- Memory: whole stack must fit a 4 GB Docker Desktop VM; ClickHouse `mem_limit: 1g`, Tempo `mem_limit: 256m`.
- Comments and commit messages in English, Conventional Commits.

## File Structure

```
crates/common/src/breaker.rs        CircuitBreaker (closed/open/half-open), tested, metrics
crates/common/src/faultflags.rs     FaultFlags + ops routes PUT/DELETE /ops/fault, tested
crates/common/src/http.rs           serve_ops gains ops fault routes
crates/proto/proto/payment.proto    PaymentService.Charge
services/payment-svc/src/main.rs    gRPC server, Redis idempotency, simulated PSP, honours FaultFlags
services/order-svc/src/payment.rs   PaymentClient with timeout + retry + CircuitBreaker
services/order-svc/src/status.rs    six statuses
services/order-svc/src/kafka.rs     also consumes orders.delivery-events
services/order-svc/src/grpc.rs      CreateOrder: insert → charge → paid → publish
services/courier-svc/src/main.rs    consumes kitchen-events(cooked) → delivery-events
services/analytics-svc/src/main.rs  consumes all event topics → ClickHouse batches; lead-time metrics
services/gateway/src/routes.rs      terminal status delivered
services/loadgen/src/main.rs        waits for delivered
control/chaos-controller/src/docker.rs   ContainerControl::exec
control/chaos-controller/src/faults.rs   FaultKind extended (in topology.rs)
control/chaos-controller/src/apply.rs    apply/heal per kind: docker stop/pause, netem, ops fault
control/chaos-controller/src/flow.rs     per-edge error ratio + p95, breaker state, lead time
control/chaos-controller/src/state.rs    derive_states with failing edges
control/chaos-controller/src/snapshot.rs edge.breaker, edge.p95_ms, lead_time_s
topology.json                            13 nodes, new edges, per-node fault lists
web/src/lib/*.svelte, api.ts             new kinds/labels, breaker badge, six-step timeline
Dockerfile, deploy/compose.yaml          iproute2, NET_ADMIN, EXEC=1, clickhouse, payment/courier/analytics
deploy/prometheus/prometheus.yml, deploy/grafana/gen_dashboards.py
tests/smoke/tests/smoke.rs               new scenarios
```

---

### Task 1: Circuit breaker and fault flags in `common`

**Files:** create `crates/common/src/breaker.rs`, `crates/common/src/faultflags.rs`; modify `crates/common/src/http.rs`, `lib.rs`.

**Produces:**
- `CircuitBreaker::new(Config { failure_threshold: u32, open_for: Duration, name: &'static str })`; `fn allow(&self, now: Instant) -> bool`; `fn on_success(&self)`; `fn on_failure(&self, now)`; `fn state(&self) -> BreakerState {Closed, Open, HalfOpen}`; gauge `circuit_breaker_state{name}` 0/1/2.
- `FaultFlags` (`Arc<RwLock<Injected { error_rate: f64, latency_ms: u64 }>>`), `async fn apply(&self) -> Result<(), Injected>`: sleeps `latency_ms` then fails with probability `error_rate`.
- `serve_ops(addr, handle, health, flags: FaultFlags, extra)` mounts `PUT /ops/fault` (JSON `{error_rate, latency_ms}`) and `DELETE /ops/fault`.

- [ ] Step 1: tests for breaker: closed allows; N failures open it; open denies until `open_for` elapsed; then half-open allows exactly one call; success closes, failure reopens.
- [ ] Step 2: implement with `Mutex<Inner { state, failures, opened_at, probing }>`.
- [ ] Step 3: tests for flags: default passes; `error_rate: 1.0` always errs; `latency_ms` measurable.
- [ ] Step 4: `cargo test -p common`; commit `feat(common): circuit breaker and injectable fault flags`.

### Task 2: payment-svc and payment path in order-svc

**Files:** `crates/proto/proto/payment.proto`, `services/payment-svc/**`, `services/order-svc/src/{payment,grpc,status,kafka,main}.rs`, `services/gateway/src/routes.rs`, `services/loadgen/src/main.rs`.

```proto
service PaymentService { rpc Charge(ChargeRequest) returns (ChargeResponse); }
message ChargeRequest { string order_id = 1; int64 amount_cents = 2; }
message ChargeResponse { string status = 1; }   // "paid" | "duplicate"
```

- [ ] Step 1: payment-svc: Redis `SET pay:{order_id} NX EX 3600` (hard dep: Redis error → `Unavailable`), simulated PSP sleep 100–400 ms, `FaultFlags` consulted first. Edges `payment-svc→redis`. Ops `:8081`, gRPC `:50052`.
- [ ] Step 2: `status.rs`: six variants; `advance` tests updated; `Delivered` terminal.
- [ ] Step 3: `payment.rs`: `PaymentClient { client, breaker: CircuitBreaker(5 failures, open 20 s, name "payment-svc"), edge }`, `charge()` with 1.5 s timeout, one retry on `Unavailable`, breaker gate → `PaymentError::CircuitOpen | Unavailable`.
- [ ] Step 4: `grpc.rs` CreateOrder: insert(accepted) → charge → `advance_status(Paid)` → publish `orders.created`; on payment failure delete row, return `Unavailable("payment unavailable")` or `Unavailable("payment circuit open")`.
- [ ] Step 5: `kafka.rs`: consume `orders.delivery-events` in the same loop (subscribe to both topics; same handler).
- [ ] Step 6: gateway terminal `delivered`; loadgen waits for `delivered` (120 s).
- [ ] Step 7: unit tests pass; commit `feat: payment service with circuit breaker`.

### Task 3: courier-svc and analytics-svc with ClickHouse

- [ ] Step 1: courier-svc: group `courier` on `orders.kitchen-events`, filter `status == "cooked"`, publish `delivering`, sleep 4–10 s, publish `delivered` on `orders.delivery-events`, partition-queue design as in kitchen. Gauge `courier_orders_in_transit`.
- [ ] Step 2: analytics-svc: group `analytics` on all three topics; buffer rows `{order_id, status, ts}`; flush every 2 s or 200 rows with `POST /?query=INSERT INTO order_events FORMAT JSONEachRow` (edge `analytics-svc→clickhouse`, proto `sql`). Startup: `CREATE TABLE IF NOT EXISTS order_events (order_id UUID, status String, ts DateTime64(3)) ENGINE = MergeTree ORDER BY ts` with retries. Every 10 s run `SELECT avg(lead) FROM (SELECT maxIf(ts,status='delivered') - minIf(ts,status='accepted') AS lead FROM order_events WHERE ts > now() - INTERVAL 5 MINUTE GROUP BY order_id HAVING lead > 0)` → gauge `analytics_lead_time_seconds`; and `SELECT count() ... status='delivered' ... 5 min` → gauge `analytics_delivered_5m`.
- [ ] Step 3: compose: clickhouse (`clickhouse/clickhouse-server:25.3`, user/password env, healthcheck `wget -qO- localhost:8123/ping`, `mem_limit: 1g`), courier-svc, analytics-svc, payment-svc; Prometheus targets.
- [ ] Step 4: commit `feat: courier and analytics services, clickhouse`.

### Task 4: Controller: new fault kinds, exec, metric-driven degradation

- [ ] Step 1: `topology.rs`: `FaultKind { Stop, Pause, Latency, Loss, AppError }` (serde `app-error`); `Node.ops_port: Option<u16>` for app faults.
- [ ] Step 2: `docker.rs`: `async fn exec(&self, name, cmd: &[&str]) -> anyhow::Result<()>` via `create_exec` (user root) + `start_exec`, error on non-zero exit.
- [ ] Step 3: `apply.rs`: `pub async fn apply(state, node, kind) -> anyhow::Result<()>` and `heal(...)`: Stop/Pause → docker; Latency → `tc qdisc replace dev eth0 root netem delay 800ms 200ms distribution normal`; Loss → `... netem loss 30%`; heal both → `tc qdisc del dev eth0 root`; AppError → `PUT http://{container}:{ops_port}/ops/fault {"error_rate":0.5}` and `DELETE` on heal. `api.rs` uses these.
- [ ] Step 4: `flow.rs`: per-edge `p95_s` from `histogram_quantile(0.95, sum by (le,src,dst) (rate(edge_request_duration_seconds_bucket[1m])))`; `breaker` from `max by (name) (circuit_breaker_state)` mapped to the edge whose `to == name`; `lead_time_s = analytics_lead_time_seconds`. `failing_edges(flow) -> Vec<(from,to)>` where `rps > 0.2 && error_ratio > 0.2 || p95_s > 1.0`.
- [ ] Step 5: `state.rs`: `derive_states(t, broken, recovering, failing_edges)`: a failing edge marks its dependent (per the same dependency rule) Degraded, hard edges propagate. Tests: failing gateway-1→order-svc degrades gateway-1 and traefik; failing soft order-svc→redis degrades order-svc only.
- [ ] Step 6: `snapshot.rs`: `EdgeSnapshot { …, p95_ms: f64, breaker: Option<String> }`, `Snapshot.lead_time_s`.
- [ ] Step 7: `cargo test -p chaos-controller`; commit `feat(chaos): netem and app faults, metric-driven degradation`.

### Task 5: Topology, image, compose plumbing

- [ ] Step 1: `topology.json`: nodes users, traefik, gateway-1/2, order-svc, redis, postgres, payment-svc, kafka, kitchen-svc, courier-svc, analytics-svc, clickhouse; faults: Rust services `stop,pause,latency,loss`; payment-svc adds `app-error`; infra `stop,pause`. New edges: order-svc→payment-svc (grpc hard), payment-svc→redis (redis hard), kafka→courier-svc, courier-svc→kafka, kafka→analytics-svc, analytics-svc→clickhouse (sql hard), kafka→order-svc already.
- [ ] Step 2: Dockerfile runtime adds `iproute2`; compose: Rust services `cap_add: [NET_ADMIN]`; socket proxy `EXEC=1`; controller `TOPOLOGY_PATH` unchanged.
- [ ] Step 3: dashboards: overview adds lead time, breaker state, funnel by status from `analytics_*`; service dashboard adds p95 in.
- [ ] Step 4: `make up`, all healthy; commit `feat(deploy): phase 2 topology and stack`.

### Task 6: Frontend

- [ ] Step 1: `api.ts` types: new FaultKind union, `p95_ms`, `breaker`, `lead_time_s`.
- [ ] Step 2: `Node.svelte`: labels `latency: 'Замедлить сеть'`, `loss: 'Терять пакеты'`, `'app-error': 'Инжектить ошибки'`; verbs for banner.
- [ ] Step 3: `Scheme.svelte`: breaker badge on the edge midpoint (`CB open` red / `half-open` yellow), edge tooltip with rps/p95; header adds «время заказа» from `lead_time_s`.
- [ ] Step 4: `Shop.svelte`: six steps; hint text per stuck stage (paid/cooking: kitchen; cooked/delivering: courier).
- [ ] Step 5: `npm run check && npm run build`; commit `feat(web): phase 2 faults, breaker and delivery timeline`.

### Task 7: Smoke tests and docs

- [ ] Step 1: new smoke tests: `payment_app_error_opens_breaker` (app-error on payment-svc → within 30 s `POST /orders` returns 503 with "circuit open" text and snapshot edge order-svc→payment-svc breaker == "open"; heal → orders succeed within 60 s); `latency_on_order_svc_degrades_gateways` (latency fault → gateway-1 `degraded` via metrics within 60 s); `order_reaches_delivered`.
- [ ] Step 2: `make smoke` green; README updated (new services, faults, ClickHouse); spec §11 phase 2 marked done, Tempo moved to phase 4 if not delivered.
- [ ] Step 3: commit `test: phase 2 smoke scenarios; docs`.

### Task 8 (time-boxed): Tempo tracing

- [ ] Step 1: `tracing-opentelemetry` + `opentelemetry-otlp` in `common::telemetry` behind env `OTLP_ENDPOINT`; W3C `traceparent` propagated over HTTP (tower middleware), gRPC (tonic interceptor) and Kafka (message headers).
- [ ] Step 2: Tempo container, Grafana datasource, gateway returns `X-Trace-Id`, Shop shows «трейс заказа» link.
- [ ] Step 3: if any of this exceeds two hours of work, stop, revert to a clean tree and record it as phase 4 in the spec.
