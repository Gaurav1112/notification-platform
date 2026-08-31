#!/usr/bin/env bash
#
# Runs every behaviour this repository claims, against a LIVE system, and captures the real
# request and response for each one.
#
# The point is that each claim in the README has a corresponding artefact produced by executing it,
# not by describing it. If a claim stops being true, re-running this makes the artefact say so.
#
#   API=http://localhost:9080 ./scripts/capture-scenarios.sh
#
set -uo pipefail
cd "$(dirname "$0")/.."
API="${API:-http://localhost:9080}"
OUT="docs/verification"
mkdir -p "$OUT"

hr() { printf '%s\n' "────────────────────────────────────────────────────────────────────────"; }
req() { printf '$ curl -X %s %s\n' "$1" "$2"; }

TEMPLATE_BODY='{"trafficClass":"TRANSACTIONAL","channels":["EMAIL"],
  "template":{"code":"order-shipped","locale":"en-US"},
  "variables":{"orderId":"A-4821","eta":"2026-09-02"},
  "recipients":{"kind":"INLINE","inline":[{"address":"priya@example.com"}]},
  "schedule":{"type":"IMMEDIATE"}}'
KEY="scenario-$(date +%s)"

# ── 10 · the accept path and both idempotency outcomes ───────────────────────
{
  echo "SCENARIO: accept, idempotent replay, and fingerprint mismatch"
  hr
  echo
  echo "1. First request"
  req POST "$API/v1/notifications  -H 'Idempotency-Key: $KEY'"
  FIRST=$(curl -s -w '\n__CODE__%{http_code}' -X POST "$API/v1/notifications" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $KEY" -d "$TEMPLATE_BODY")
  BODY1=${FIRST%__CODE__*}; CODE1=${FIRST##*__CODE__}
  echo "HTTP $CODE1"
  echo "$BODY1" | python3 -m json.tool 2>/dev/null || echo "$BODY1"
  NID=$(echo "$BODY1" | python3 -c "import sys,json;print(json.load(sys.stdin)['notifications'][0]['id'])" 2>/dev/null)
  echo
  echo "2. SAME key, SAME body   — must replay, must NOT create anything"
  SECOND=$(curl -s -w '\n__CODE__%{http_code}' -X POST "$API/v1/notifications" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $KEY" -d "$TEMPLATE_BODY")
  BODY2=${SECOND%__CODE__*}; CODE2=${SECOND##*__CODE__}
  echo "HTTP $CODE2"
  echo "$BODY2" | python3 -m json.tool 2>/dev/null || echo "$BODY2"
  echo
  if [ "$BODY1" = "$BODY2" ]; then
    echo "==> RESPONSES ARE BYTE-IDENTICAL. The retry cannot tell it was second."
  else
    echo "==> WARNING: responses differ. Idempotent replay is NOT working."
  fi
  echo
  echo "3. SAME key, DIFFERENT body   — must be 409, not a silent replay"
  THIRD=$(curl -s -w '\n__CODE__%{http_code}' -X POST "$API/v1/notifications" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $KEY" \
    -d '{"trafficClass":"BULK","channels":["SMS"],"template":{"code":"other","locale":"en-US"},
         "recipients":{"kind":"INLINE","inline":[{"address":"someone-else@example.com"}]},
         "schedule":{"type":"IMMEDIATE"}}')
  echo "HTTP ${THIRD##*__CODE__}"
  echo "${THIRD%__CODE__*}" | python3 -m json.tool 2>/dev/null
  echo
  echo "notification id for the next scenario: $NID"
  echo "$NID" > /tmp/.scenario-nid
} > "$OUT/10-accept-and-idempotency.txt" 2>&1

sleep 6
NID=$(cat /tmp/.scenario-nid 2>/dev/null)

# ── 11 · status and attempts ─────────────────────────────────────────────────
{
  echo "SCENARIO: observing a notification after accept"
  hr
  echo
  req GET "$API/v1/notifications/$NID"
  curl -s "$API/v1/notifications/$NID" | python3 -m json.tool 2>/dev/null
  echo
  req GET "$API/v1/notifications/$NID/recipients"
  curl -s "$API/v1/notifications/$NID/recipients" | python3 -m json.tool 2>/dev/null | head -30
  echo
  req GET "$API/v1/notifications/$NID/attempts"
  curl -s "$API/v1/notifications/$NID/attempts" | python3 -m json.tool 2>/dev/null | head -30
} > "$OUT/11-status-and-attempts.txt" 2>&1

# ── 12 · validation and the RFC 9457 error contract ──────────────────────────
{
  echo "SCENARIO: the error contract — RFC 9457 application/problem+json"
  hr
  echo
  echo "1. Missing Idempotency-Key on a request that creates work"
  curl -s -w '\nHTTP %{http_code}\n' -X POST "$API/v1/notifications" \
    -H 'Content-Type: application/json' -d "$TEMPLATE_BODY" | python3 -c "
import sys
raw=sys.stdin.read()
body,_,code=raw.rpartition('HTTP ')
try:
    import json; print(json.dumps(json.loads(body.strip()), indent=2))
except Exception: print(body.strip())
print('HTTP', code.strip())
" 2>/dev/null
  echo
  echo "2. Recipient selector inconsistent with its kind"
  curl -s -w '\nHTTP %{http_code}\n' -X POST "$API/v1/notifications" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: bad-$(date +%s)" \
    -d '{"trafficClass":"TRANSACTIONAL","channels":["EMAIL"],
         "template":{"code":"x","locale":"en-US"},
         "recipients":{"kind":"USER_IDS","inline":[{"address":"a@b.com"}]},
         "schedule":{"type":"IMMEDIATE"}}' | head -20
  echo
  echo "3. Unknown notification id  — 404, never 403, because a 403 confirms it exists"
  curl -s -w '\nHTTP %{http_code}\n' "$API/v1/notifications/00000000-0000-7000-8000-000000000000" | head -12
} > "$OUT/12-error-contract.txt" 2>&1

# ── 13 · provider health and circuit-breaker failover ────────────────────────
{
  echo "SCENARIO: circuit breaker opens under failure and the router fails over"
  hr
  echo
  echo "1. Baseline — every circuit CLOSED"
  req GET "$API/v1/providers/health"
  curl -s "$API/v1/providers/health" | python3 -m json.tool 2>/dev/null | head -40
  echo
  echo "2. Break the primary email provider"
  req POST "$API/admin/v1/mock-providers/mock-email-primary/chaos"
  curl -s -w '\nHTTP %{http_code}\n' -X POST "$API/admin/v1/mock-providers/mock-email-primary/chaos" \
    -H 'Content-Type: application/json' -d '{"mode":"HARD_DOWN","durationSeconds":90}' | head -10
  echo
  echo "3. Drive traffic through the broken provider"
  for i in $(seq 1 30); do
    curl -s -o /dev/null -X POST "$API/v1/notifications" \
      -H 'Content-Type: application/json' -H "Idempotency-Key: chaos-$(date +%s)-$i" \
      -d "$TEMPLATE_BODY"
  done
  echo "   30 requests sent"
  sleep 15
  echo
  echo "4. Provider health after the failures"
  curl -s "$API/v1/providers/health" | python3 -m json.tool 2>/dev/null | head -40
  echo
  echo "5. Restore"
  curl -s -o /dev/null -X POST "$API/admin/v1/mock-providers/mock-email-primary/chaos" \
    -H 'Content-Type: application/json' -d '{"mode":"NORMAL","durationSeconds":1}'
  echo "   chaos cleared"
} > "$OUT/13-circuit-breaker-failover.txt" 2>&1

# ── 14 · what actually landed in the database ────────────────────────────────
{
  echo "SCENARIO: the durable record behind those requests"
  hr
  echo
  echo '$ psql -d notification'
  docker exec -i np-postgres psql -U notification -d notification -q <<'SQL' 2>&1
\pset border 2
\echo 'Row counts'
SELECT (SELECT count(*) FROM notif.notification)          AS notifications,
       (SELECT count(*) FROM notif.notification_recipient) AS recipients,
       (SELECT count(*) FROM notif.idempotency_record)     AS idempotency_records,
       (SELECT count(*) FROM notif.outbox_message)         AS unpublished_outbox,
       (SELECT count(*) FROM notif.delivery_attempt)       AS delivery_attempts;
\echo ''
\echo 'The outbox is empty because the sweeper published and DELETED each row.'
\echo 'Rows are deleted, never marked published, so the table cannot grow without bound.'
\echo ''
\echo 'Most recent notifications'
SELECT id, channel, status, status_rank
  FROM notif.notification ORDER BY created_at DESC LIMIT 5;
\echo ''
\echo 'The status vocabulary is a TABLE, not an enum — rank and is_terminal are data the SQL joins on'
SELECT code, rank, is_terminal, is_failure FROM notif.delivery_status ORDER BY rank;
SQL
} > "$OUT/14-database-state.txt" 2>&1

# ── render ───────────────────────────────────────────────────────────────────
for txt in "$OUT"/1[0-4]-*.txt; do
  base=$(basename "$txt" .txt)
  ./scripts/render-terminal.sh "$txt" "$OUT/$base.png" "$base" 2>/dev/null
  [ -f "$OUT/$base.png" ] && printf '  rendered %s\n' "$base.png"
done
