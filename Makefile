COMPOSE := docker compose --project-directory . -f deploy/compose.yaml
DEV     := $(COMPOSE) -f deploy/compose.dev.yaml
PROD    := $(COMPOSE) -f deploy/compose.prod.yaml

.PHONY: up down restart logs ps build test smoke web fmt lint dashboards prod-up prod-down

up: ## build images and start the whole stack locally
	$(DEV) up -d --build

down: ## stop and remove containers (keeps images)
	$(DEV) down --remove-orphans

restart: down up

logs:
	$(DEV) logs -f --tail=100 $(S)

ps:
	$(DEV) ps

build:
	$(DEV) build

test: ## unit tests
	cargo test --workspace

smoke: ## integration tests against a running stack (BASE_URL, ADMIN_TOKEN)
	cargo test -p smoke -- --ignored --test-threads=1 --nocapture

web:
	cd web && npm ci && npm run check && npm run build

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace --all-targets -- -D warnings

dashboards:
	python3 deploy/grafana/gen_dashboards.py

prod-up:
	$(PROD) up -d --build

prod-down:
	$(PROD) down --remove-orphans
