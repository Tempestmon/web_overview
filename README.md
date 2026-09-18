# Chaos Panel

A public playground for breaking a real distributed system and watching what
happens. One page shows the architecture of a small food-delivery backend
with live traffic; every component has a **Break** button. The other page is
the food-delivery app itself, so you can place an order and then watch it
stall, fail or slow down depending on what you broke.

Everything is real: Kafka, Redis, PostgreSQL, gRPC and HTTP between Rust
services, a load balancer in front of two replicas. Faults are real too:
containers get stopped or frozen through the Docker API.

```
                 ┌──────── traefik (LB) ────────┐
   users ────────┤                              │
                 └─► gateway-1 ──┐   gateway-2 ◄┘
                                 │ gRPC
                                 ▼                gRPC + circuit breaker
                             order-svc ─────────────────► payment-svc ──► redis (idempotency)
                              │  │ ▲ └─SQL──► postgres
                              │  │ │  └─cache─► redis
                      produce │  │ │ consume
                              ▼  ▼ │
                       kafka: orders.created → kitchen-svc → orders.kitchen-events
                                                  │                 │
                                                  ▼                 ▼
                              courier-svc ─► orders.delivery-events   analytics-svc ─► clickhouse
```

Order lifecycle: `accepted → paid → cooking → cooked → delivering → delivered`.
Payment is synchronous (the customer learns immediately), everything after
it is asynchronous through Kafka.

## Run it locally

Requirements: Docker with Compose v2, ~2 GB of RAM for the stack.

```bash
cp .env.example .env      # optional, defaults work locally
make up                   # builds one image with every service, starts 14 containers
```

| URL | What |
|---|---|
| http://localhost:8080/ | the panel |
| http://localhost:8080/order | the food-delivery app |
| http://localhost:8080/grafana/ | Grafana (anonymous viewer; Tempo traces under Explore) |
| http://localhost:9090/ | Prometheus |

The first build compiles the Rust workspace inside Docker and takes several
minutes; later builds reuse the cached dependency layer.

## How a fault works

1. The panel posts `{node, kind}` to `chaos-controller`.
2. The controller checks the shared-world rules: one fault at a time, a
   cooldown after each, one fault per IP per two minutes. `ADMIN_TOKEN` in
   the `X-Admin-Token` header bypasses them.
3. It applies the fault. `stop`/`pause` go through the Docker API
   (`docker-socket-proxy` exposes only `/containers` and `/exec`);
   `latency`/`loss` run `tc qdisc ... netem` inside the target container;
   `app-error` calls the service's own `PUT /ops/fault` so half of its
   responses fail. Which node offers which fault is declared in
   `topology.json`.
4. Every second it publishes a snapshot over SSE: container health from
   Docker, per-edge request rates from Prometheus, and derived states.
   A broken node is red; everything that transitively depends on it through
   a hard edge is yellow. Soft edges (the Redis cache) degrade one hop only.
   Yellow also comes from live metrics: an edge with more than 20% errors,
   or a request/response edge with p95 above one second, degrades its
   caller even when every container is up. The `order-svc → payment-svc`
   edge shows the circuit breaker state (`CB open` / `half-open`).
5. When the TTL expires, or the owner clicks *Fix*, the container comes back.
   A controller restart reconciles anything left stopped.

Node ids, container names, edge semantics and screen positions all live in
one file, `topology.json`, read by both the controller and the frontend.

## Tracing

Every service exports spans to Tempo over OTLP/HTTP when `OTLP_ENDPOINT`
is set. The W3C `traceparent` travels in HTTP headers, gRPC metadata and
Kafka message headers, so one order is one trace from the gateway through
payment, kitchen, courier and analytics. The gateway returns the trace id
in `x-trace-id`; the shop page links each order to its trace in Grafana.

## Development

```bash
make test        # unit tests (Rust)
make lint        # clippy with warnings denied
make web         # type-check and build the Svelte app
make smoke       # end-to-end tests against a running stack
make dashboards  # regenerate Grafana dashboards from deploy/grafana/gen_dashboards.py
```

Layout: `crates/common` (metrics, health, config, circuit breaker, fault
flags), `crates/proto` (gRPC contracts), `services/*` (data plane and load
generator),
`control/chaos-controller`, `web/` (Svelte), `deploy/` (compose, Traefik,
Prometheus, Grafana, Ansible), `tests/smoke`.

## Deploy to a server

`deploy/compose.prod.yaml` adds TLS via Let's Encrypt and restart policies.
`deploy/ansible/site.yml` installs Docker on a Debian/Ubuntu host, syncs the
repository and starts the production overlay:

```bash
cp deploy/ansible/inventory.example deploy/ansible/inventory   # set the host
cp .env.example .env                                            # set DOMAIN, ACME_EMAIL, ADMIN_TOKEN
ansible-playbook -i deploy/ansible/inventory deploy/ansible/site.yml
```

## Roadmap

See `docs/superpowers/specs/2026-09-17-chaos-panel-design.md` for the design
and the phased roadmap. Phases 1 and 2 are done (see the spec's roadmap
table); next are public hardening (rate limits, nightly reset, alerts on
the stand itself), tracing with Tempo, and guided failure scenarios.
