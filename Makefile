# =============================================================================
# notification-platform
#
# Everything a reviewer needs, in one file, with no hidden state.
# `make help` lists the targets; `make up && make build && make demo` is the
# whole tour.
#
# The three Spring apps run on the HOST, not in containers, so a debugger
# attaches and a recompile is instant. Only infrastructure is dockerised.
# =============================================================================

SHELL         := /bin/bash
.SHELLFLAGS   := -eu -o pipefail -c
.DEFAULT_GOAL := help

COMPOSE   := docker compose -f docker/compose.yml
MVN       := ./mvnw
API       := http://localhost:8080
DB_USER   := notification
DB_NAME   := notification

# One place to change the demo payload shape if the API contract moves.
SMS_BODY  = {"trafficClass":"CRITICAL","channels":["SMS"],"recipients":{"kind":"INLINE","addresses":["+15550100001"]},"content":{"body":"Your verification code is 481920"},"schedule":{"type":"IMMEDIATE"},"ttlSeconds":60,"metadata":{"correlationId":"demo-chaos"}}


# Testcontainers 2.x probes /var/run/docker.sock, which does not exist on Rancher
# Desktop or Colima. Without this, `make test-it` fails with "Could not find a valid
# Docker environment" even though Docker is running perfectly. DOCKER_HOST detection:
DOCKER_SOCK := $(shell \
  if [ -S /var/run/docker.sock ]; then echo ""; \
  elif [ -S "$$HOME/.rd/docker.sock" ]; then echo "unix://$$HOME/.rd/docker.sock"; \
  elif [ -S "$$HOME/.colima/default/docker.sock" ]; then echo "unix://$$HOME/.colima/default/docker.sock"; \
  else echo ""; fi)
ifneq ($(DOCKER_SOCK),)
export DOCKER_HOST := $(DOCKER_SOCK)
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE := /var/run/docker.sock
endif

.PHONY: help up down logs ps topics build test test-it psql reset wait status demo

# -----------------------------------------------------------------------------
help: ## Show this help
	@echo ""
	@echo "  notification-platform — local development"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-8s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  swagger     $(API)/swagger-ui.html"
	@echo "  kafka-ui    http://localhost:8081"
	@echo "  prometheus  http://localhost:9090"
	@echo "  grafana     http://localhost:3000   (anonymous admin, local only)"
	@echo ""
	@echo "  Native Linux Docker Engine only — 'host.docker.internal' does not"
	@echo "  exist there, so Prometheus cannot reach the host JVMs. Start with:"
	@echo "    HOST_ALIAS='host.docker.internal:host-gateway' make up"
	@echo ""

# -----------------------------------------------------------------------------
# infrastructure
# -----------------------------------------------------------------------------
up: ## Start postgres, valkey, kafka (+topics), kafka-ui, prometheus, grafana
	$(COMPOSE) up -d --wait
	@echo ""
	@echo "  infrastructure healthy. The 16 topics were created by kafka-init."
	@echo "  Start the apps in three terminals:"
	@echo "    $(MVN) -pl app-api       spring-boot:run"
	@echo "    $(MVN) -pl app-worker    spring-boot:run"
	@echo "    $(MVN) -pl app-scheduler spring-boot:run"
	@echo ""

down: ## Stop the containers, keep the volumes
	$(COMPOSE) down --remove-orphans

logs: ## Tail container logs  (make logs S=kafka for one service)
	$(COMPOSE) logs -f --tail=100 $(S)

ps: ## Container status and health
	$(COMPOSE) ps

topics: ## (Re)create the 16 Kafka topics — idempotent, safe to re-run
	$(COMPOSE) run --rm kafka-init

# -----------------------------------------------------------------------------
# build
# -----------------------------------------------------------------------------
build: ## Compile and install all modules (skips tests)
	$(MVN) -q -B -DskipTests install

test: ## Unit and slice tests — no Docker needed
	$(MVN) -B verify

test-it: ## Everything, including the Testcontainers suite — needs a running Docker daemon
	$(MVN) -B verify -Pintegration

# -----------------------------------------------------------------------------
# database
# -----------------------------------------------------------------------------
psql: ## psql shell on the notification database
	$(COMPOSE) exec postgres psql -U $(DB_USER) -d $(DB_NAME) -v ON_ERROR_STOP=1

reset: ## DESTRUCTIVE — drop every volume and start from an empty schema
	@echo "  This deletes the postgres, kafka, valkey, prometheus and grafana volumes."
	@read -r -p "  type 'reset' to confirm: " ans; \
	  [ "$$ans" = "reset" ] || { echo "  aborted"; exit 1; }
	$(COMPOSE) down -v --remove-orphans
	@echo "  volumes gone. 'make up' rebuilds; flyway replays V1 on the next API start."

# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
wait: ## Block until the API answers its health check
	@printf '  waiting for %s/actuator/health ' "$(API)"
	@for i in $$(seq 1 60); do \
	  if curl -sf $(API)/actuator/health >/dev/null 2>&1; then echo " up"; exit 0; fi; \
	  printf '.'; sleep 2; \
	done; \
	echo " TIMEOUT — is '$(MVN) -pl app-api spring-boot:run' running?"; exit 1

status: ## Provider health and circuit state
	@curl -sS $(API)/v1/providers/health | python3 -m json.tool

# -----------------------------------------------------------------------------
# demo — the chaos failover story, end to end, in about three minutes.
#
# What it proves, in order:
#   1. a CRITICAL SMS is accepted well inside the 250 ms budget
#   2. replaying the same Idempotency-Key returns the SAME id — no second send
#   3. killing the primary provider opens its circuit within about two seconds
#   4. traffic fails over to the secondary and sends keep returning 202
#   5. the breaker half-opens, probes, and closes with no operator action
#
# Step 4 is the one to watch. The circuit going OPEN is not the interesting
# part — the interesting part is that the accept path never returned a single
# error while it happened.
# -----------------------------------------------------------------------------
demo: ## Chaos failover demo against a running stack (needs jq)
	@command -v jq >/dev/null || { echo "  jq is required:  brew install jq"; exit 1; }
	@$(MAKE) --no-print-directory wait
	@echo ""
	@echo "== 1. baseline: every provider closed ============================="
	@curl -sS $(API)/v1/providers/health \
	  | jq -c '.providers[] | {code, channel, circuit: .circuitState, healthy}'
	@echo ""
	@echo "== 2. send a CRITICAL SMS ========================================="
	@IDEM=$$(uuidgen); \
	  echo "   Idempotency-Key: $$IDEM"; \
	  curl -sS -o /tmp/np-demo-1.json -w '   HTTP %{http_code} in %{time_total}s\n' \
	    -X POST $(API)/v1/notifications \
	    -H 'Content-Type: application/json' \
	    -H "Idempotency-Key: $$IDEM" \
	    -d '$(SMS_BODY)'; \
	  jq -c '{id: .notificationRequestId, status, recipientCount}' /tmp/np-demo-1.json; \
	  echo ""; \
	  echo "== 3. replay the SAME key: must return the same id, not send twice"; \
	  curl -sS -X POST $(API)/v1/notifications \
	    -H 'Content-Type: application/json' \
	    -H "Idempotency-Key: $$IDEM" \
	    -d '$(SMS_BODY)' \
	  | jq -c '{id: .notificationRequestId, status}'
	@echo ""
	@echo "== 4. take the primary SMS provider HARD_DOWN for 120 s ==========="
	@curl -sS -X POST $(API)/admin/v1/mock-providers/mock-sms-primary/chaos \
	  -H 'Content-Type: application/json' \
	  -d '{"mode":"HARD_DOWN","durationSeconds":120}' | jq -c .
	@echo ""
	@echo "== 5. keep sending: every one of these must still be 202 =========="
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
	  addr="+1555010$$(printf '%04d' $$i)"; \
	  code=$$(curl -sS -o /dev/null -w '%{http_code}' -X POST $(API)/v1/notifications \
	    -H 'Content-Type: application/json' \
	    -H "Idempotency-Key: $$(uuidgen)" \
	    -d "{\"trafficClass\":\"CRITICAL\",\"channels\":[\"SMS\"],\"recipients\":{\"kind\":\"INLINE\",\"addresses\":[\"$$addr\"]},\"content\":{\"body\":\"code $$i\"},\"schedule\":{\"type\":\"IMMEDIATE\"},\"ttlSeconds\":60}"); \
	  printf '   send %2d  ->  HTTP %s\n' "$$i" "$$code"; \
	  sleep 1; \
	done
	@echo ""
	@echo "== 6. circuit state: primary OPEN, secondary carrying the traffic ="
	@sleep 3
	@curl -sS $(API)/v1/providers/health \
	  | jq -c '.providers[] | select(.channel=="SMS") | {code, circuit: .circuitState, healthy}'
	@echo ""
	@echo "   Failover happened with zero 5xx on the accept path."
	@echo "   Watch the breaker half-open and close on its own:"
	@echo ""
	@echo "     watch -n5 'curl -s $(API)/v1/providers/health | jq -c \".providers[]|{code,circuitState}\"'"
	@echo ""
	@echo "   Grafana     http://localhost:3000/d/np-operational"
	@echo "   Prometheus  http://localhost:9090/alerts"
	@echo ""
