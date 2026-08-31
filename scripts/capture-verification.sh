#!/usr/bin/env bash
#
# Captures verification evidence as real terminal output, then renders it to PNG.
#
# Everything in docs/verification/ is produced by this script from live commands.
# Nothing is hand-written. Re-run it after any significant change; if a claim in
# the README stops being true, the regenerated image will say so.
#
#   ./scripts/capture-verification.sh          # capture + render
#   ./scripts/capture-verification.sh --text   # capture only, skip PNG rendering
#
set -uo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)
OUT="$ROOT/docs/verification"
mkdir -p "$OUT"

RENDER=1
[ "${1:-}" = "--text" ] && RENDER=0

say() { printf '\n\033[1;36m▸ %s\033[0m\n' "$1"; }

# ── 1. build + tests ─────────────────────────────────────────────────────────
say "build and test suite"
{
  echo "\$ ./mvnw -B clean verify"
  ./mvnw -B clean verify 2>&1 \
    | grep -E '^\[INFO\] (Tests run:.*Skipped: [0-9]+$|-{20,}|Reactor Summary|notification-platform |Platform :: |App :: |BUILD )|^\[ERROR\]' \
    | sed 's/\[INFO\] //'
} > "$OUT/01-build.txt" 2>&1

{
  echo "\$ ./mvnw -B clean verify -Pintegration     # includes Testcontainers"
  ./mvnw -B clean verify -Pintegration 2>&1 \
    | grep -E '^\[INFO\] (Tests run:.*Skipped: [0-9]+$|BUILD )|^\[ERROR\]' \
    | sed 's/\[INFO\] //'
} > "$OUT/02-build-integration.txt" 2>&1

# ── 2. repository shape ──────────────────────────────────────────────────────
say "repository shape"
{
  echo "\$ scripts/capture-verification.sh  — repository inventory"
  echo
  printf 'main java files    %s\n'  "$(find . -path '*/src/main/java/*' -name '*.java' | wc -l | tr -d ' ')"
  printf 'test java files    %s\n'  "$(find . -path '*/src/test/java/*' -name '*.java' | wc -l | tr -d ' ')"
  printf 'java lines         %s\n'  "$(find . -name '*.java' -not -path './*/target/*' -exec cat {} + | wc -l | tr -d ' ')"
  printf 'sql lines          %s\n'  "$(find . -name '*.sql' -not -path './*/target/*' -exec cat {} + | wc -l | tr -d ' ')"
  printf 'maven modules      %s\n'  "$(grep -c '<module>' pom.xml)"
  printf 'markdown docs      %s\n'  "$(find docs -name '*.md' | wc -l | tr -d ' ')"
  printf 'ADRs               %s\n'  "$(ls docs/adr/ADR-*.md 2>/dev/null | wc -l | tr -d ' ')"
  printf 'commits            %s\n'  "$(git rev-list --count HEAD)"
  echo
  echo '$ git log --oneline'
  git log --oneline | head -12
} > "$OUT/03-repo.txt" 2>&1

# ── 3. schema, applied to a real PostgreSQL 18.6 ─────────────────────────────
say "schema against PostgreSQL 18.6"
PG=np-verify-pg
docker rm -f "$PG" >/dev/null 2>&1
docker run -d --name "$PG" -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=notification \
  -p 55433:5432 postgres:18.6 >/dev/null 2>&1
for _ in $(seq 1 60); do docker exec "$PG" pg_isready -U postgres -q 2>/dev/null && break; sleep 1; done

{
  echo "\$ psql -f V1__baseline.sql          # applied with ON_ERROR_STOP=1"
  docker exec -i "$PG" psql -U postgres -d notification -v ON_ERROR_STOP=1 -q \
    < platform-persistence/src/main/resources/db/migration/V1__baseline.sql 2>&1 \
    && echo "applied cleanly, no errors"
  echo
  docker exec -i "$PG" psql -U postgres -d notification -q <<'SQL'
\pset border 2
SELECT 'tables'              AS object, count(*) FROM information_schema.tables
        WHERE table_schema='notif' AND table_type='BASE TABLE'
UNION ALL SELECT 'partitioned parents', count(*) FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='notif' AND c.relkind='p'
UNION ALL SELECT 'partitions', count(*) FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='notif'
UNION ALL SELECT 'indexes', count(*) FROM pg_indexes WHERE schemaname='notif'
UNION ALL SELECT 'check constraints', count(*) FROM pg_constraint co
        JOIN pg_namespace n ON n.oid=co.connamespace WHERE n.nspname='notif' AND co.contype='c';
SQL
} > "$OUT/04-schema.txt" 2>&1

# ── 4. the two behaviours the design rests on ────────────────────────────────
say "security constraint and monotonic guard"
{
  echo "── A credentials_ref CHECK physically rejects a pasted API key ──"
  echo
  docker exec -i "$PG" psql -U postgres -d notification -q <<'SQL' 2>&1
INSERT INTO notif.provider (code, display_name, channel, vendor)
     VALUES ('mock-sms-primary','Mock SMS','SMS','mock');
\echo '$ INSERT ... credentials_ref = ''SK1234567890abcdefTHISISAKEY''   -- a real-looking key
INSERT INTO notif.provider_configuration (provider_id, credentials_ref, retry_policy_id)
     VALUES (1, 'SK1234567890abcdefTHISISAKEY', 1);
\echo
\echo '$ INSERT ... credentials_ref = ''mock:sms-primary''               -- a reference, not a secret
INSERT INTO notif.provider_configuration (provider_id, credentials_ref, retry_policy_id)
     VALUES (1, 'mock:sms-primary', 1) RETURNING id AS accepted_id;
SQL
  echo
  echo
  echo "── B the monotonic guard, run as the application runs it ──"
  echo
  docker exec -i "$PG" psql -U postgres -d notification -q <<'SQL' 2>&1
\pset border 2
INSERT INTO notif.tenant (slug, display_name) VALUES ('acme','Acme Corp');
INSERT INTO notif.notification (id, request_id, tenant_id, channel, traffic_class,
                                status, status_rank, expires_at)
VALUES ('01998f2a-7c31-7a04-9e12-6f0b3c1d5a89','01998f2a-7c31-7a04-9e12-6f0b3c1d5a88',
        1,'SMS','CRITICAL','SENT',60, now()+interval '1 hour');

PREPARE apply(text,smallint,uuid) AS
UPDATE notif.notification n SET status=$1, status_rank=$2, status_at=now()
 WHERE n.id=$3
   AND n.created_at >= current_date - 1 AND n.created_at < current_date + 1
   AND n.status_rank < $2
   AND NOT EXISTS (SELECT 1 FROM notif.delivery_status d
                    WHERE d.code=n.status AND d.is_terminal)
RETURNING n.status AS applied;

\echo '1. DELIVERED(80) over SENT(60)                     -> expect 1 row'
EXECUTE apply('DELIVERED', 80, '01998f2a-7c31-7a04-9e12-6f0b3c1d5a89');
\echo '2. a LATE SENT(60) arriving after DELIVERED        -> expect 0 rows'
EXECUTE apply('SENT', 60, '01998f2a-7c31-7a04-9e12-6f0b3c1d5a89');
\echo '3. a duplicate DELIVERED webhook                   -> expect 0 rows'
EXECUTE apply('DELIVERED', 80, '01998f2a-7c31-7a04-9e12-6f0b3c1d5a89');
\echo '4. BOUNCED(85) after DELIVERED (not terminal)      -> expect 1 row'
EXECUTE apply('BOUNCED', 85, '01998f2a-7c31-7a04-9e12-6f0b3c1d5a89');
\echo '5. anything after BOUNCED (terminal)               -> expect 0 rows'
EXECUTE apply('UNKNOWN', 90, '01998f2a-7c31-7a04-9e12-6f0b3c1d5a89');
\echo
\echo 'final state:'
SELECT status, status_rank FROM notif.notification
 WHERE id='01998f2a-7c31-7a04-9e12-6f0b3c1d5a89';
SQL
} > "$OUT/05-behaviour.txt" 2>&1

docker rm -f "$PG" >/dev/null 2>&1

# ── 5. render each capture to PNG ────────────────────────────────────────────
if [ "$RENDER" -eq 1 ]; then
  say "rendering PNGs"
  command -v node >/dev/null || { echo "node not found, skipping render"; exit 0; }
  for txt in "$OUT"/*.txt; do
    base=$(basename "$txt" .txt)
    title=$(head -1 "$txt" | sed 's/^\$ //' | cut -c1-90)
    node "$ROOT/scripts/render-terminal.mjs" "$txt" "$OUT/$base.png" "$base" || echo "  render failed: $base"
    [ -f "$OUT/$base.png" ] && echo "  ✓ $base.png"
  done
fi

say "done — see docs/verification/"
ls -1 "$OUT"
