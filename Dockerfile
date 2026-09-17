# One image, every binary: the services share 95% of their dependency
# graph, so building them together is far cheaper than five images.

FROM node:24-alpine AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npm run build

FROM rust:1.98-trixie AS chef
RUN apt-get update && apt-get install -y --no-install-recommends \
      pkg-config protobuf-compiler cmake build-essential \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --workspace --recipe-path recipe.json
COPY . .
COPY --from=web /web/dist ./web/dist
RUN cargo build --release --workspace --bins \
    && mkdir -p /out \
    && cp target/release/gateway target/release/order-svc target/release/kitchen-svc \
          target/release/loadgen target/release/chaos-controller /out/

FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 app
COPY --from=builder /out/ /usr/local/bin/
USER app
