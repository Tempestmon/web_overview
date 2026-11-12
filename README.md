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
| http://localhost:8080/grafana/ | Grafana (anonymous viewer; traces on the *Traces* dashboard) |
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
   `app-error` calls the service's own `PUT /ops/fault` so 90% of its
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

## Load

The *нагрузка* buttons in the panel header multiply the number of virtual
customers (×1, ×3, ×10) for two minutes. The controller forwards the
request to `loadgen` (`PUT /ops/load`), which resizes its crowd and reverts
on its own when the TTL ends, so a controller restart cannot leave the
stand overloaded. One visitor drives the load at a time; ×1 is open to
everyone.

Customers wait for their order before placing the next one, so ×10 people
is not ×10 requests: once the kitchen saturates, each customer simply
orders less often. Watch the Kafka backlog and the order lead time instead
of the request rate.

## Six experiments

The panel offers each of these as a guided tour under *Что будет, если…*:
it injects the fault and ticks off the effects as they show up on the
scheme. They are ordered so that each one adds a single idea to the
previous ones. Open `/order` in a second tab and place an order while a
tour runs to see the same failure from the customer's side.

| # | Break this | What you see | The idea |
|---|---|---|---|
| 1 | stop `kitchen-svc` | Kitchen is red, orders are still accepted, the Kafka backlog grows and drains after recovery | A queue turns a consumer outage into a delay instead of an error |
| 2 | stop `gateway-1` | Traffic moves to `gateway-2`, customers see no errors | Stateless replicas behind a health-checking balancer fail over on their own |
| 3 | stop `postgres` | `order-svc`, both gateways and Traefik turn yellow, red particles run up to the balancer | In a synchronous chain a storage outage is a user-facing outage at once |
| 4 | stop `redis` | The menu still loads, payments fail and the breaker opens | The same component is a soft dependency for one client and a hard one for another |
| 5 | inject errors into `payment-svc` | Every container is up, the edge shows `CB open`, `order-svc` turns yellow | A circuit breaker makes a failing dependency fail fast instead of eating the caller's time |
| 6 | add latency to `order-svc` | No container is down, yet the gateways turn yellow once p95 passes one second | Slow is broken too, and only latency metrics can see it |

Experiments 1 and 3 are the pair to compare: the same `stop`, applied to
an asynchronous consumer and to a synchronous dependency, gives a delay in
one case and errors in the other. Experiments 5 and 6 are the pair where
health checks stay green and the yellow comes purely from Prometheus.

The mechanics behind the table, for reading the code afterwards:

- **Queue (1).** `order-svc` publishes to `orders.created` and returns; it
  depends on the broker, not on the consumer. `kitchen-svc` commits an
  offset only after an order is cooked, so a stop loses nothing.
- **Failover (2).** Traefik probes `/healthz` on each gateway every two
  seconds and drops a replica that fails.
- **Soft and hard (4).** `order-svc` treats a Redis error on the menu cache
  as a cache miss and reads PostgreSQL. `payment-svc` keeps idempotency
  keys in Redis and refuses to charge without them, because a retry could
  then charge twice.
- **Circuit breaker (5).** The payment client in `order-svc` has a 1.5 s
  timeout and one retry. Five failures in a row open the breaker for 20 s;
  after that one probe request decides whether it closes. See
  `crates/common/src/breaker.rs`.
- **Latency (6).** The controller reads p95 per edge from Prometheus over
  a one-minute window, which is why the yellow appears and disappears with
  a delay.

To follow one order through all of this, click the trace link on the
order page: it opens the order's trace in Grafana, with the Kafka hops
included.

## Tracing

Every service exports spans to Tempo over OTLP/HTTP when `OTLP_ENDPOINT`
is set. The W3C `traceparent` travels in HTTP headers, gRPC metadata and
Kafka message headers, so one order is one trace from the gateway through
payment, kitchen, courier and analytics. The gateway returns the trace id
in `x-trace-id`; the shop page links each order to its trace on the
*Traces* dashboard. A dashboard rather than Explore, because anonymous
visitors are Viewers and Grafana does not let Viewers into Explore.

## Development

```bash
make test        # unit tests (Rust)
make lint        # clippy with warnings denied
make web         # type-check, unit-test and build the Svelte app
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
and the phased roadmap. Phases 1 and 2 are done, tracing with Tempo
included. Phase 3 (public hardening) is in progress: the controller
force-restarts a component that stays down and Prometheus carries alert
rules about the stand itself; the nightly reset and the VPS deployment are
still to come. Phase 4 (guided tours, particles that follow RPS, a layout
that works on a phone, this README as a walkthrough) is done.
