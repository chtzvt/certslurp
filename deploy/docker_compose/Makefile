VERSION ?= v0.0.0-local
GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dirty)
BUILD_DATE ?= $(shell date -u +%FT%TZ)
export VERSION GIT_COMMIT BUILD_DATE

.PHONY: build up up-fast bootstrap etcd head workers down nuke logs ps ctl shell restart tail-head tail-workers

# Build all local images
build:
	docker compose build

# Bring up etcd (bootstrap user/role), then head + workers
up: etcd bootstrap head workers
	@echo "Certslurp cluster up"

# Same as up, but skip bootstrap (useful if etcd data already has users/roles)
up-fast: etcd head workers
	@echo "⚡ cluster up (no bootstrap)"

# Start only etcd (detached), used by 'up' and 'up-fast'
etcd:
	docker compose up -d --wait etcd

# One-shot: create app user/role & enable auth (idempotent)
bootstrap:
	docker compose --profile bootstrap run --rm etcd-bootstrap

# Start head only (detached)
head:
	docker compose up -d head

# Start workers only (detached)
workers:
	docker compose up -d worker-1 worker-2

# Tear everything down
down:
	docker compose down -v

# HARD reset: stop, remove volumes, and delete the named etcd volume
nuke:
	-docker compose down -v
	-docker volume rm certslurp-local_etcd-data || true
	@echo "Wiped volumes; next 'make up' will re-bootstrap etcd"

# Logs
logs:
	docker compose logs -f --tail=200

tail-head:
	docker compose logs -f --tail=200 head

tail-workers:
	docker compose logs -f --tail=200 worker-1 worker-2

ps:
	docker compose ps

# Restart head + workers, preserves etcd data
restart:
	docker compose restart head worker-1 worker-2

# Interactive toolbox shell
ctl:
	docker compose --profile debug run --rm ctl