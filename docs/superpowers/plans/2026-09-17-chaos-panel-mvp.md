# Chaos Panel MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A locally runnable food-delivery demo system plus a chaos control panel that can stop/pause any component, shows the dependency cascade on an SVG scheme, and links every node to Grafana.

**Architecture:** Cargo workspace with a shared `common` crate (metrics, health, config), a `proto` crate (tonic codegen), four data-plane/loadgen services and one `chaos-controller` that owns `topology.json`, drives Docker through `docker-socket-proxy`, reads edge RPS from Prometheus and streams state snapshots over SSE to a Svelte SPA embedded in its binary. Everything runs under one Docker Compose file; Traefik is the single entrypoint.

**Tech Stack:** Rust 1.98, axum 0.8, tonic 0.14, sqlx 0.9 (postgres, runtime-tokio), redis 1.x, rdkafka 0.39 (dynamic-linking), bollard 0.21, metrics + metrics-exporter-prometheus, Svelte 5 + Vite 8 + TypeScript, Traefik v3.3, apache/kafka 3.9.1 (KRaft), Postgres 17, Redis 7, Prometheus v3, Grafana 11, kafka-exporter v1.9, tecnativa/docker-socket-proxy 0.3.

**Spec:** `docs/superpowers/specs/2026-09-17-chaos-panel-design.md`

## Global Constraints

- Node ids in `topology.json` equal Docker container names; chaos-controller only ever touches containers listed there.
- Every service exposes `GET /healthz` (200 when dependencies reachable enough to serve) and `GET /metrics` (Prometheus text).
- Every inter-component call records `edge_requests_total{src,dst,proto,outcome}` and `edge_request_duration_seconds{src,dst,proto}` via `common::edge`.
- Service identity comes from env `SERVICE_ID` (e.g. `gateway-1`), used as `src` label.
- Faults: one active at a time, TTL default 60 s, cooldown 15 s, per-IP one fault per 120 s; `ADMIN_TOKEN` header bypasses limits.
- Single entry: Traefik on host port 8080. Routes: `/api/shop/*` → gateway-1/2, `/grafana/*` → grafana, everything else → chaos-controller.
- Memory budget: whole stack under ~2 GB; compose sets `mem_limit` on infra containers.
- Comments and commit messages in English, Conventional Commits.
- Order statuses, monotonic: `accepted < cooking < cooked`. `cooked` is terminal in MVP.
- Kafka topics: `orders.created` (order-svc → kitchen), `orders.kitchen-events` (kitchen → order-svc).

## File Structure

```
Cargo.toml                       workspace members, shared [workspace.dependencies]
rust-toolchain.toml              channel = "1.98"
topology.json                    nodes + edges, single source of truth
Makefile                         up/down/build/test/smoke/web
Dockerfile                       multi-stage: web build, cargo-chef, ARG BIN
.dockerignore, .gitignore, .env.example
crates/common/src/lib.rs         re-exports
crates/common/src/config.rs      env helpers: required(), or_default()
crates/common/src/telemetry.rs   init tracing + Prometheus recorder handle
crates/common/src/edge.rs        Edge recorder: time + count a call with labels
crates/common/src/http.rs        axum router with /healthz + /metrics, serve()
crates/proto/build.rs, proto/order.proto, src/lib.rs
services/gateway/src/main.rs     wiring
services/gateway/src/routes.rs   /api/shop handlers + SSE
services/gateway/src/client.rs   OrderService gRPC client wrapper with edge metrics
services/order-svc/src/main.rs   wiring
services/order-svc/src/grpc.rs   OrderService impl
services/order-svc/src/repo.rs   Postgres queries
services/order-svc/src/menu_cache.rs  Redis cache-aside
services/order-svc/src/kafka.rs  producer + kitchen-events consumer
services/order-svc/src/status.rs pure status ordering (tested)
services/order-svc/migrations/0001_init.sql
services/kitchen-svc/src/main.rs consumer → cook → producer
services/loadgen/src/main.rs     virtual users
control/chaos-controller/src/main.rs
control/chaos-controller/src/topology.rs   load + validate topology.json (tested)
control/chaos-controller/src/state.rs      derive_states() (tested)
control/chaos-controller/src/faults.rs     FaultBook: lock/ttl/cooldown/ip limit (tested)
control/chaos-controller/src/docker.rs     ContainerControl trait + bollard impl
control/chaos-controller/src/health.rs     poll containers → broken set
control/chaos-controller/src/flow.rs       Prometheus queries → edge rps, counters
control/chaos-controller/src/api.rs        axum routes + SSE + static files
control/chaos-controller/src/snapshot.rs   Snapshot struct shared with frontend
web/                             Svelte app (package.json, vite.config.ts, src/)
web/src/lib/api.ts               types mirroring Snapshot, EventSource client
web/src/lib/Scheme.svelte        SVG topology + particles
web/src/lib/Node.svelte, Edge.svelte, Particles.ts
web/src/routes/Panel.svelte, Shop.svelte
deploy/compose.yaml, compose.dev.yaml, compose.prod.yaml
deploy/traefik/traefik.yml, dynamic.yml
deploy/prometheus/prometheus.yml
deploy/grafana/provisioning/{datasources,dashboards}/*.yml, dashboards/*.json
deploy/ansible/site.yml, inventory.example
tests/smoke/                     ignored integration tests against a running stack
```

---

### Task 0: Workspace skeleton and common crate

**Files:** `Cargo.toml`, `rust-toolchain.toml`, `.gitignore`, `crates/common/**`

**Produces:**
- `common::config::{required(name) -> String, or_default(name, default) -> String}`
- `common::telemetry::init(service_id: &str) -> PrometheusHandle` (installs tracing subscriber + metrics recorder)
- `common::edge::Edge { src, dst, proto }` with `async fn observe<T, E>(&self, f: impl Future<Output=Result<T,E>>) -> Result<T,E>` recording `edge_requests_total` and `edge_request_duration_seconds`
- `common::http::serve_ops(addr, handle, extra: Router) -> Result<()>` serving `/healthz`, `/metrics` plus `extra`

- [ ] Step 1: write workspace `Cargo.toml` with `[workspace.dependencies]` pinned: tokio 1 (full), axum 0.8, tonic 0.14, prost 0.14, tonic-build 0.14, sqlx 0.9 (runtime-tokio, postgres, uuid, chrono, json, migrate), redis 1 (tokio-comp, connection-manager), rdkafka 0.39 (dynamic-linking), bollard 0.21, metrics 0.24, metrics-exporter-prometheus 0.18, tracing, tracing-subscriber (env-filter, json), serde, serde_json, anyhow, thiserror, reqwest 0.13 (json), uuid (v4, serde), chrono (serde), tokio-stream, async-stream, rust-embed 8, governor 0.10, futures.
- [ ] Step 2: write `crates/common` with the four modules. Test `edge::observe` records counter (use `metrics_util::debugging::DebuggingRecorder`); test `config::or_default`.
- [ ] Step 3: `cargo test -p common` passes. Commit `chore: bootstrap workspace and common crate`.

### Task 1: Proto crate

**Files:** `crates/proto/proto/order.proto`, `build.rs`, `src/lib.rs`

**Produces:** `proto::order::{order_service_client::OrderServiceClient, order_service_server::{OrderService, OrderServiceServer}, GetMenuRequest, Menu, MenuItem, CreateOrderRequest, OrderLine, Order, GetOrderRequest}`

```proto
syntax = "proto3"; package order;
service OrderService {
  rpc GetMenu(GetMenuRequest) returns (Menu);
  rpc CreateOrder(CreateOrderRequest) returns (Order);
  rpc GetOrder(GetOrderRequest) returns (Order);
}
message GetMenuRequest {}
message MenuItem { int64 id = 1; string name = 2; int64 price_cents = 3; }
message Menu { repeated MenuItem items = 1; bool from_cache = 2; }
message OrderLine { int64 item_id = 1; int32 qty = 2; }
message CreateOrderRequest { repeated OrderLine lines = 1; string client_id = 2; }
message GetOrderRequest { string id = 1; }
message Order { string id = 1; string status = 2; int64 total_cents = 3; repeated OrderLine lines = 4; string created_at = 5; string updated_at = 6; }
```

- [ ] Step 1: create crate, `cargo build -p proto`. Commit `feat(proto): define OrderService`.

### Task 2: order-svc

**Files:** `services/order-svc/**`

**Consumes:** proto, common. **Produces:** gRPC on `:50051`, ops HTTP on `:8081`, topics per constraints.

- [ ] Step 1: `status.rs`: `enum OrderStatus { Accepted, Cooking, Cooked }` with `FromStr`, `as_str`, `Ord`; `fn advance(current, incoming) -> Option<OrderStatus>` returning `Some(incoming)` only if `incoming > current`. Tests: accepted→cooking Some, cooked→cooking None, parse roundtrip.
- [ ] Step 2: migration `0001_init.sql`: `menu_items(id bigserial pk, name text, price_cents bigint)` seeded with 6 items; `orders(id uuid pk, client_id text, status text, total_cents bigint, lines jsonb, created_at timestamptz default now(), updated_at timestamptz default now())`; index on `created_at`.
- [ ] Step 3: `repo.rs`: `Repo(PgPool)` with `list_menu`, `insert_order(id, client_id, lines, total)`, `get_order(id)`, `advance_status(id, status) -> bool`. Each wrapped in `Edge{src, dst:"postgres", proto:"sql"}.observe`.
- [ ] Step 4: `menu_cache.rs`: `MenuCache(ConnectionManager)` with `get() -> Option<Vec<MenuItem>>` / `put(items, ttl 30s)`; every call under `Edge{dst:"redis", proto:"redis"}` with a 300 ms timeout; errors logged and treated as miss (soft dependency).
- [ ] Step 5: `kafka.rs`: `Producer::publish_created(order_id)` (edge `dst:"kafka", proto:"kafka"`), `run_kitchen_events_consumer(repo)` reading `orders.kitchen-events` `{order_id, status}` and calling `repo.advance_status` (edge `src:"kafka", dst:SERVICE_ID`).
- [ ] Step 6: `grpc.rs`: implement the three RPCs; `CreateOrder` computes total from menu, inserts, publishes; publish failure → status `Unavailable` and the order row is deleted (no half-created orders). Counter `orders_created_total`.
- [ ] Step 7: `main.rs`: run migrations, build pool, redis manager, producer, spawn consumer, serve tonic + ops. `/healthz` 200 iff Postgres `SELECT 1` succeeds.
- [ ] Step 8: `cargo test -p order-svc`; commit `feat(order-svc): menu, orders, kafka round-trip`.

### Task 3: kitchen-svc

- [ ] Step 1: consumer group `kitchen` on `orders.created`; on message publish `{order_id, status:"cooking"}`, sleep uniform 5–15 s (env `COOK_MIN_MS`/`COOK_MAX_MS`), publish `status:"cooked"`; commit offset after `cooked`. Edge metrics on consume and produce. Gauge `kitchen_orders_in_progress`.
- [ ] Step 2: ops server on `:8081`; `/healthz` 200 iff last consumer poll error-free within 30 s.
- [ ] Step 3: commit `feat(kitchen-svc): cook orders from kafka`.

### Task 4: gateway

- [ ] Step 1: `client.rs`: `OrderClient` wrapping `OrderServiceClient<Channel>` with 2 s timeout and `Edge{dst:"order-svc", proto:"grpc"}`.
- [ ] Step 2: `routes.rs`: `GET /api/shop/menu`, `POST /api/shop/orders` `{lines:[{item_id,qty}]}` → 201 `{id,status,total_cents}`, `GET /api/shop/orders/{id}`, `GET /api/shop/orders/{id}/events` SSE polling gRPC every 1 s, emitting `status` events on change, closing on `cooked` or after 180 s. Gauge `gateway_sse_connections`. gRPC `Unavailable`/timeout → 503 JSON `{error:"order service unavailable"}`. Inbound requests recorded as `Edge{src:"traefik", dst:SERVICE_ID, proto:"http"}` via middleware.
- [ ] Step 3: `/healthz` always 200 while the process runs (Traefik health-check semantics: the replica itself is alive). Test route handlers with a mocked `OrderClient` trait: 201 on create, 503 on unavailable.
- [ ] Step 4: commit `feat(gateway): shop API and order status SSE`.

### Task 5: loadgen

- [ ] Step 1: env `USERS` (20), `BASE_URL`. Each user loop: menu → random 1–3 lines → create → poll `GET /orders/{id}` each 1 s until `cooked` or 90 s → sleep 2–6 s. Gauge `loadgen_active_users`, counters `loadgen_orders_total{outcome}`. Backoff 5 s on error. Ops on `:8081`.
- [ ] Step 2: commit `feat(loadgen): virtual users`.

### Task 6: topology.json and chaos-controller core (pure logic, tested)

**Files:** `topology.json`, `control/chaos-controller/src/{topology,state,faults,snapshot}.rs`

- [ ] Step 1: `topology.json` with nodes `users`(kind:"actor", no container), `traefik`, `gateway-1`, `gateway-2`, `order-svc`, `postgres`, `redis`, `kafka`, `kitchen-svc`; each with `label`, `kind` (`lb|service|db|cache|broker|actor`), `container`, `pos`, `grafana` (dashboard uid), `faults` (`[]` for users/traefik, `["stop","pause"]` otherwise), `ops_port`. Edges: users→traefik (http, `query` override on `traefik_service_requests_total`), traefik→gateway-1, traefik→gateway-2 (http), gateway-N→order-svc (grpc, hard), order-svc→postgres (sql, hard), order-svc→redis (redis, soft), order-svc→kafka (kafka, hard), kafka→kitchen-svc (kafka, hard), kitchen-svc→kafka (kafka, hard), kafka→order-svc (kafka, hard).
- [ ] Step 2: `topology.rs`: structs `Topology { nodes: Vec<Node>, edges: Vec<Edge> }`, `Node { id, label, kind, container: Option<String>, pos, grafana, faults: Vec<FaultKind>, ops_port: Option<u16> }`, `Edge { from, to, proto: Proto, dep: Dep, query: Option<String> }`; `Topology::load(path)`, `validate()` (edges reference known nodes, ids unique). Test with the real file.
- [ ] Step 3: `state.rs`: `enum NodeState { Healthy, Broken, Degraded, Recovering }`; `fn dependencies(t: &Topology) -> HashMap<NodeId, Vec<(NodeId, Dep)>>` per rule (http/grpc/sql/redis: from depends on to; kafka: both endpoints depend on the broker end, where broker = node with kind `broker`); `fn derive_states(t, broken: &HashSet<NodeId>, recovering: &HashSet<NodeId>) -> HashMap<NodeId, NodeState>` BFS over reverse dependencies; soft edge propagates one hop only. Tests: kitchen broken → kafka/order-svc/gateway healthy; kafka broken → order-svc, kitchen, gateways degraded; redis broken → order-svc degraded, gateways healthy; postgres broken → order-svc, gateway-1/2 degraded, traefik degraded (traefik→gateway is http hard), users degraded too? No: `users` is kind actor, excluded from derivation.
- [ ] Step 4: `faults.rs`: `FaultBook::new(Limits{ttl, cooldown, per_ip})`; `try_start(node, kind, ip, now, is_admin) -> Result<Fault, Refusal>` with `Refusal::{Busy{remaining}, Cooldown{remaining}, RateLimited{retry_after}}`; `expired(now) -> Option<Fault>`; `finish(id, now)`. Tests: second fault while active → Busy; after finish → Cooldown; same IP within 120 s → RateLimited; admin bypasses; expired after ttl.
- [ ] Step 5: `snapshot.rs`: `Snapshot { ts, nodes: Vec<NodeSnapshot{id,state,fault:Option<FaultKind>}>, edges: Vec<EdgeSnapshot{from,to,rps,error_rps}>, active_fault: Option<ActiveFault{id,node,kind,ends_at}>, cooldown_until: Option<ts>, people_online, orders_per_min, kafka_lag }` serde Serialize.
- [ ] Step 6: `cargo test -p chaos-controller`; commit `feat(chaos): topology, state derivation, fault book`.

### Task 7: chaos-controller runtime (docker, health, flow, api)

- [ ] Step 1: `docker.rs`: `trait ContainerControl { stop, start, pause, unpause, status(name) -> ContainerStatus{Running, Paused, Exited, Missing, Unhealthy} }`, `BollardControl::connect(url)`. Status uses `State.Status` + `State.Health.Status`.
- [ ] Step 2: `health.rs`: every 2 s inspect all container nodes → sets `broken` (Exited/Paused/Unhealthy/Missing) and `recovering` (Running but no `healthy` yet after fault end). On startup: `reconcile()` starts Exited and unpauses Paused containers from the topology.
- [ ] Step 3: `flow.rs`: `PromClient::edge_rates() -> HashMap<(src,dst), (rps, error_rps)>` via `sum by (src,dst,outcome) (rate(edge_requests_total[20s]))`; per-edge `query` override evaluated separately; `people_online = sum(loadgen_active_users) + sum(gateway_sse_connections)`; `orders_per_min = sum(increase(orders_created_total[1m]))`; `kafka_lag = sum(kafka_consumergroup_lag)`.
- [ ] Step 4: `api.rs`: routes per spec §7; `POST /api/faults` maps `Refusal` → 409/429 with `retry_after_s`; applies fault via `ContainerControl` (`stop`/`pause`), schedules heal by TTL; `DELETE` heals early (owner ip or admin). SSE emits `Snapshot` every 1 s from a `watch` channel. Static SPA from `web/dist` via `rust-embed`, fallback to `index.html` for unknown non-`/api` paths. Client IP from `X-Forwarded-For` (Traefik sets it).
- [ ] Step 5: `main.rs` wiring, env: `DOCKER_URL`, `PROMETHEUS_URL`, `TOPOLOGY_PATH`, `ADMIN_TOKEN`, `FAULT_TTL_S`, `COOLDOWN_S`, `IP_LIMIT_S`.
- [ ] Step 6: commit `feat(chaos): docker faults, health, prometheus flow, api`.

### Task 8: Frontend

- [ ] Step 1: `npm create vite@latest web -- --template svelte-ts`; add `svelte-check`. `vite.config.ts` proxies `/api` and `/grafana` to `http://localhost:8080` in dev.
- [ ] Step 2: `api.ts`: TS types mirroring `Topology` and `Snapshot`; `connectStream(onSnapshot)` with EventSource auto-reconnect; `startFault(node, kind)`, `stopFault(id)`.
- [ ] Step 3: `Scheme.svelte`: `<svg viewBox="0 0 1200 700">`, edges as `<path>` between node centers, `Node.svelte` renders rect + label + state ring (`healthy` black outline, `degraded` yellow fill, `broken` red fill, `recovering` dashed), a "Сломать ▾" menu with the node's `faults`, a Grafana link `/grafana/d/{uid}?var-service={id}`. `Particles.ts` keeps per-edge particle arrays, spawns `rps` particles/s (capped 40), moves them with rAF; red particles for `error_rps`.
- [ ] Step 4: `Panel.svelte`: header with people online, orders/min, kafka lag, active fault banner with countdown and "Починить" (DELETE). Busy/cooldown reflected on buttons (disabled + remaining seconds).
- [ ] Step 5: `Shop.svelte` at `/order`: menu cards, cart, "Заказать", status timeline fed by `/api/shop/orders/{id}/events`; error banner on 503; "Меню грузится медленно" hint if menu request > 1 s.
- [ ] Step 6: black-and-white style; `npm run check && npm run build`; commit `feat(web): panel and shop pages`.

### Task 9: Docker, Compose, observability, Makefile

- [ ] Step 1: `Dockerfile`: stage `web` (node:24-alpine, `npm ci && npm run build`), stage `chef` (rust:1.98-trixie + cargo-chef + `librdkafka-dev cmake pkg-config protobuf-compiler`), `planner`, `builder` (copies web/dist before `cargo build --release --bin $BIN`), runtime `debian:trixie-slim` with `librdkafka1 ca-certificates curl` and non-root user. `HEALTHCHECK` via `curl -f localhost:$OPS_PORT/healthz`.
- [ ] Step 2: `deploy/compose.yaml`: all services with `container_name` equal to topology ids, healthchecks for postgres (`pg_isready`), redis (`redis-cli ping`), kafka (`kafka-broker-api-versions.sh`), `mem_limit`s; Traefik file provider; `docker-socket-proxy` with `CONTAINERS=1 POST=1`; kafka-exporter; prometheus; grafana with anonymous viewer under `/grafana`; `chaos-controller` mounts `topology.json` read-only.
- [ ] Step 3: `deploy/traefik/dynamic.yml`: routers `shop` (PathPrefix `/api/shop`, priority 20) → service `gateway` (two servers, healthCheck `/healthz` every 2 s); `grafana` (PathPrefix `/grafana`, priority 20); `panel` (PathPrefix `/`, priority 1) → chaos-controller. Metrics on `:8082`.
- [ ] Step 4: `deploy/prometheus/prometheus.yml` scraping every service, traefik, kafka-exporter (5 s interval).
- [ ] Step 5: Grafana provisioning: datasource, dashboards `overview` (uid `overview`) and `service` (uid `service`, variable `service` from `label_values(edge_requests_total, src)`), plus `kafka` (uid `kafka`). Topology `grafana` field references these uids.
- [ ] Step 6: `Makefile`: `up` (`docker compose -f deploy/compose.yaml -f deploy/compose.dev.yaml up -d --build`), `down`, `logs`, `ps`, `test`, `smoke`, `web`, `fmt`, `lint`. `.env.example` with `ADMIN_TOKEN`, `USERS`, `DOMAIN`.
- [ ] Step 7: `make up`; verify: `curl :8080/api/shop/menu`, panel at `http://localhost:8080/`, Grafana at `/grafana`. Commit `feat(deploy): compose stack with traefik, prometheus, grafana`.

### Task 10: Smoke tests and README

- [ ] Step 1: crate `tests/smoke` (`#[ignore]` tests, `BASE_URL` env): `order_reaches_cooked` (create → poll ≤ 60 s); `stop_kitchen_stalls_orders` (POST fault stop kitchen-svc with admin token → new order stays `accepted` for 20 s → snapshot shows kitchen `broken`, order-svc `healthy` → DELETE fault → order reaches `cooked` ≤ 90 s); `stop_redis_degrades_order_svc` (fault → menu still 200 → snapshot order-svc `degraded`, gateway-1 `healthy` → heal).
- [ ] Step 2: `make smoke` runs them; all green.
- [ ] Step 3: `README.md` (English): what it is, architecture picture, `make up`, URLs, how faults work, how to deploy (Ansible), roadmap pointer. Commit `docs: README and smoke tests`.

### Task 11: Deployability

- [ ] Step 1: `deploy/compose.prod.yaml`: Traefik entrypoints 80/443 with ACME resolver (`DOMAIN`, `ACME_EMAIL` env), `restart: always`, no dev ports, `chaos-controller` env from `.env`.
- [ ] Step 2: `deploy/ansible/site.yml`: role-less playbook: install Docker CE from official repo, create `/opt/chaos-panel`, sync repo (`synchronize` excluding target/ node_modules/), copy `.env`, `docker compose ... up -d --build`. `inventory.example`.
- [ ] Step 3: `.github/workflows/ci.yml`: fmt, clippy, test, web check/build, `docker build` of one image.
- [ ] Step 4: commit `feat(deploy): prod overlay, ansible playbook, ci`.
